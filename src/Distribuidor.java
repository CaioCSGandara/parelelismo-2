import java.io.*;
import java.net.Socket;
import java.util.*;

public class Distribuidor {

    // CONFIGURAÇÃO DOS SERVIDORES (IPs e Portas)
    private static final String[][] SERVIDORES = {
            {"127.0.0.1", "12345"}
    };

    public static void main(String[] args) {
        Scanner console = new Scanner(System.in);
        try {
            System.out.println("--- DISTRIBUIDOR INICIADO ---");

            // 1. GERAÇÃO DO VETOR GIGANTE
            int tamanhoTotal = 30_000_000;
            byte[] vetorGigante = new byte[tamanhoTotal];
            new Random().nextBytes(vetorGigante);
            System.out.println("Vetor gerado com " + String.format("%,d", tamanhoTotal) + " elementos.");

            // 2. DIVISÃO DO VETOR EM PARTES
            int numServidores = SERVIDORES.length;
            List<byte[]> partes = dividirVetor(vetorGigante, numServidores);

            // 3. PREPARAÇÃO DAS THREADS DE CONEXÃO
            // Usamos arrays para guardar as threads e os resultados (memória compartilhada)
            Thread[] threadsConexao = new Thread[numServidores];
            byte[][] resultadosRecebidos = new byte[numServidores][];

            long inicio = System.currentTimeMillis();

            // Loop de criação e disparo das threads
            for (int i = 0; i < numServidores; i++) {
                final int index = i; // Variável final para usar dentro do lambda
                String ip = SERVIDORES[i][0];
                int porta = Integer.parseInt(SERVIDORES[i][1]);
                byte[] parteParaEnviar = partes.get(i);

                // Definição da lógica da thread usando Lambda
                threadsConexao[i] = new Thread(() -> {
                    try (Socket socket = new Socket(ip, porta);
                         ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                         ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                        System.out.println("[D] Thread " + index + " enviando dados para " + ip);
                        out.writeObject(new Pedido(parteParaEnviar));
                        out.flush();

                        // Aguarda resposta
                        Object resp = in.readObject();
                        if (resp instanceof Resposta r) {
                            // Salva o resultado no array compartilhado na posição correta
                            resultadosRecebidos[index] = r.getNumerosOrdenados();
                            System.out.println("[D] Thread " + index + " recebeu resposta de " + ip);
                        }
                    } catch (Exception e) {
                        System.err.println("Erro na thread " + index + ": " + e.getMessage());
                    }
                });

                // Inicia a thread imediatamente
                threadsConexao[i].start();
            }

            // 4. SINCRONIZAÇÃO (JOIN)
            // A thread main espera todas as threads de conexão terminarem
            List<byte[]> listaOrdenadaParaMerge = new ArrayList<>();
            for (int i = 0; i < numServidores; i++) {
                try {
                    threadsConexao[i].join(); // Bloqueia até a thread i terminar

                    if (resultadosRecebidos[i] != null) {
                        listaOrdenadaParaMerge.add(resultadosRecebidos[i]);
                    } else {
                        System.err.println("[D] Falha: Nenhum resultado recebido do servidor " + i);
                    }
                } catch (InterruptedException e) {
                    System.err.println("Thread principal interrompida.");
                }
            }

            // 5. MERGE FINAL (JUNÇÃO DOS RESULTADOS)
            System.out.println("[D] Iniciando merge final dos " + listaOrdenadaParaMerge.size() + " vetores...");
            byte[] vetorFinal = juntarResultadosComThreads(listaOrdenadaParaMerge);

            long fim = System.currentTimeMillis();
            System.out.println("Tempo total de execução: " + (fim - inicio) / 1000.0 + " segundos");

            // 6. SALVAR ARQUIVO E ENCERRAR
            System.out.print("Digite o nome do arquivo para salvar: ");
            salvarArquivo(vetorFinal, console.nextLine());

            enviarEncerramento();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // --- MÉTODOS AUXILIARES (Sem classes internas) ---

    // Divide o vetor original em N partes
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

    // Faz o merge dos vetores usando Threads para paralelizar as junções 2 a 2
    private static byte[] juntarResultadosComThreads(List<byte[]> listas) throws InterruptedException {
        while (listas.size() > 1) {
            int qtdPares = listas.size() / 2;
            Thread[] threadsMerge = new Thread[qtdPares];
            byte[][] resultadosRodada = new byte[qtdPares][]; // Array para guardar o resultado de cada par

            // Dispara threads para cada par
            for (int i = 0; i < qtdPares; i++) {
                final int idx = i;
                byte[] v1 = listas.get(i * 2);
                byte[] v2 = listas.get(i * 2 + 1);

                threadsMerge[i] = new Thread(() -> {
                    // Usa a classe utilitária estática para fazer o merge
                    resultadosRodada[idx] = MergeUtils.merge(v1, v2);
                });
                threadsMerge[i].start();
            }

            // Lista para a próxima iteração
            List<byte[]> proximaFase = new ArrayList<>();

            // Se o número de vetores era ímpar, o último passa direto para a próxima fase
            if (listas.size() % 2 != 0) {
                proximaFase.add(listas.get(listas.size() - 1));
            }

            // Aguarda (join) todas as threads de merge terminarem
            for (int i = 0; i < qtdPares; i++) {
                threadsMerge[i].join();
                // Adiciona o resultado fundido no início da lista da próxima fase
                proximaFase.add(0, resultadosRodada[i]);
            }

            listas = proximaFase;
        }
        return listas.isEmpty() ? new byte[0] : listas.get(0);
    }

    private static void enviarEncerramento() {
        System.out.println("[D] Enviando sinal de encerramento...");
        for (String[] server : SERVIDORES) {
            try (Socket s = new Socket(server[0], Integer.parseInt(server[1]));
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream())) {
                out.writeObject(new ComunicadoEncerramento());
            } catch (Exception e) {
                // Ignora erros se o servidor já estiver fechado
            }
        }
    }

    private static void salvarArquivo(byte[] dados, String nome) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(nome))) {
            for (byte b : dados) {
                bw.write(String.valueOf(b));
                bw.newLine();
            }
            System.out.println("Arquivo salvo com sucesso.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}