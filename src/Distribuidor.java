import java.io.*;
import java.net.Socket;
import java.util.*;

public class Distribuidor {

    private static final String[][] SERVIDORES = {
            {"127.0.0.1", "8082"},
            {"192.168.0.234", "8082"}
    };

    public static void main(String[] args) {
        Scanner console = new Scanner(System.in);
        try {
            System.out.println("--- DISTRIBUIDOR INICIADO ---");

            int tamanhoTotal = 30_000_000;
            byte[] vetorGigante = new byte[tamanhoTotal];
            new Random().nextBytes(vetorGigante);
            System.out.println("Vetor gerado com " + String.format("%,d", tamanhoTotal) + " elementos.");

            System.out.println("Gravando vetor original não ordenado em disco...");
            salvarArquivo(vetorGigante, "vetor_original.txt");
            System.out.println("Vetor original salvo em 'vetor_original.txt'. Iniciando distribuição...");

            int numServidores = SERVIDORES.length;
            List<byte[]> partes = dividirVetor(vetorGigante, numServidores);


            Thread[] threadsConexao = new Thread[numServidores];
            byte[][] resultadosRecebidos = new byte[numServidores][];

            long inicio = System.currentTimeMillis();

            for (int i = 0; i < numServidores; i++) {
                final int index = i;
                String ip = SERVIDORES[i][0];
                int porta = Integer.parseInt(SERVIDORES[i][1]);
                byte[] parteParaEnviar = partes.get(i);

                threadsConexao[i] = new Thread(() -> {
                    try (Socket socket = new Socket(ip, porta);
                         ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                         ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                        System.out.println("[D] Thread " + index + " enviando para " + ip + ":" + porta);
                        out.writeObject(new Pedido(parteParaEnviar));
                        out.flush();

                        Object resp = in.readObject();
                        if (resp instanceof Resposta r) {
                            resultadosRecebidos[index] = r.getNumerosOrdenados();
                            System.out.println("[D] Thread " + index + " recebeu resposta de " + ip);
                        }
                    } catch (Exception e) {
                        System.err.println("Erro na thread " + index + " (" + ip + ":" + porta + "): " + e.getMessage());
                    }
                });
                threadsConexao[i].start();
            }

            List<byte[]> listaOrdenadaParaMerge = new ArrayList<>();
            for (int i = 0; i < numServidores; i++) {
                try {
                    threadsConexao[i].join();
                    if (resultadosRecebidos[i] != null) {
                        listaOrdenadaParaMerge.add(resultadosRecebidos[i]);
                    } else {
                        System.err.println("[D] Falha grave: Nenhum resultado recebido do servidor " + i);
                    }
                } catch (InterruptedException e) {
                    System.err.println("Thread interrompida.");
                }
            }

            System.out.println("[D] Todos os servidores responderam. Iniciando merge final...");
            byte[] vetorFinal = juntarResultadosComThreads(listaOrdenadaParaMerge);

            long fim = System.currentTimeMillis();
            System.out.println("Tempo total de ordenação (excluindo gravação inicial): " + (fim - inicio) / 1000.0 + "s");

            System.out.print("Digite o nome do arquivo FINAL ordenado (ex: ordenado.txt): ");
            salvarArquivo(vetorFinal, console.nextLine());

            enviarEncerramento();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void salvarArquivo(byte[] dados, String nome) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(nome))) {
            for (byte b : dados) {
                bw.write(String.valueOf(b));
                bw.newLine();
            }
            System.out.println("Arquivo '" + nome + "' gravado com sucesso.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<byte[]> dividirVetor(byte[] total, int partes) {
        List<byte[]> lista = new ArrayList<>();
        int base = total.length / partes;
        int resto = total.length % partes;
        int pos = 0;
        for(int i=0; i<partes; i++) {
            int tam = base + (i < resto ? 1 : 0);
            byte[] p = new byte[tam];
            System.arraycopy(total, pos, p, 0, tam);
            lista.add(p);
            pos += tam;
        }
        return lista;
    }

    private static byte[] juntarResultadosComThreads(List<byte[]> listas) throws InterruptedException {
        while (listas.size() > 1) {
            int qtdPares = listas.size() / 2;
            Thread[] threadsMerge = new Thread[qtdPares];
            byte[][] resultadosRodada = new byte[qtdPares][];

            // Dispara threads para cada par
            for (int i = 0; i < qtdPares; i++) {
                final int idx = i;
                byte[] v1 = listas.get(i * 2);
                byte[] v2 = listas.get(i * 2 + 1);
                threadsMerge[i] = new Thread(() -> {
                    resultadosRodada[idx] = MergeUtils.merge(v1, v2);
                });
                threadsMerge[i].start();
            }

            List<byte[]> proximaFase = new ArrayList<>();
            if (listas.size() % 2 != 0) {
                proximaFase.add(listas.get(listas.size() - 1));
            }

            for (int i = 0; i < qtdPares; i++) {
                threadsMerge[i].join();
                proximaFase.add(0, resultadosRodada[i]);
            }
            listas = proximaFase;
        }
        return listas.isEmpty() ? new byte[0] : listas.get(0);
    }

    private static void enviarEncerramento() {
        System.out.println("[D] Enviando sinal de encerramento para os servidores...");
        for (String[] server : SERVIDORES) {
            try (Socket s = new Socket(server[0], Integer.parseInt(server[1]));
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream())) {
                out.writeObject(new ComunicadoEncerramento());
            } catch (Exception e) {
            }
        }
    }
}