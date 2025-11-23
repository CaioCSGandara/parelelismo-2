import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Receptor {
    private static final int PORTA = 8082;

    public static void main(String[] args) {
        System.out.println("[R] Servidor Receptor ouvindo na porta " + PORTA);

        try (ServerSocket serverSocket = new ServerSocket(PORTA)) {
            while (true) {
                Socket cliente = serverSocket.accept();
                new Thread(() -> tratarConexao(cliente)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void tratarConexao(Socket socket) {
        try (
                ObjectOutputStream transmissor = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream receptor = new ObjectInputStream(socket.getInputStream())
        ) {
            boolean running = true;
            while (running) {
                try {
                    Object obj = receptor.readObject();

                    if (obj instanceof Pedido pedido) {
                        byte[] vetor = pedido.getNumeros();
                        System.out.println("[R] Pedido recebido. Tamanho: " + vetor.length);

                        int numProcessadores = Runtime.getRuntime().availableProcessors();
                        System.out.println("[R] Usando " + numProcessadores + " threads para processar.");
                        List<byte[]> partesParaOrdenar = dividirVetor(vetor, numProcessadores);

                        Thread[] threads = new Thread[numProcessadores];
                        byte[][] resultadosOrdenacao = new byte[numProcessadores][];


                        for (int i = 0; i < numProcessadores; i++) {
                            if (i < partesParaOrdenar.size()) {
                                final int index = i;
                                byte[] parte = partesParaOrdenar.get(i);
                                threads[i] = new Thread(() -> {
                                    resultadosOrdenacao[index] = MergeUtils.sequentialMergeSort(parte);
                                });
                                threads[i].start();
                            }
                        }

                        List<byte[]> partesOrdenadas = new ArrayList<>();
                        for (int i = 0; i < numProcessadores; i++) {
                            if (threads[i] != null) {
                                threads[i].join();
                                partesOrdenadas.add(resultadosOrdenacao[i]);
                            }
                        }

                        byte[] resultadoFinal = executarMergeParalelo(partesOrdenadas);
                        transmissor.writeObject(new Resposta(resultadoFinal));
                        transmissor.flush();
                        System.out.println("[R] Resposta enviada para o cliente.");

                    } else if (obj instanceof ComunicadoEncerramento) {
                        System.out.println("[R] Comunicado de encerramento recebido. Fechando conexão.");
                        running = false;
                    }
                } catch (EOFException e) {
                    System.out.println("[R] Cliente desconectou (EOF). Sessão finalizada.");
                    running = false;
                }
            }
        } catch (Exception e) {
            System.err.println("[R] Erro geral: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try { socket.close(); } catch (IOException e) {}
        }
    }


    private static List<byte[]> dividirVetor(byte[] total, int partes) {
        List<byte[]> lista = new ArrayList<>();
        int tamanhoBase = total.length / partes;
        int resto = total.length % partes;
        int inicio = 0;

        for (int i = 0; i < partes; i++) {
            int tamanho = tamanhoBase + (i < resto ? 1 : 0);
            if (tamanho > 0) {
                byte[] pedaco = new byte[tamanho];
                System.arraycopy(total, inicio, pedaco, 0, tamanho);
                lista.add(pedaco);
                inicio += tamanho;
            }
        }
        return lista;
    }

    private static byte[] executarMergeParalelo(List<byte[]> listas) throws InterruptedException {
        while (listas.size() > 1) {
            int pares = listas.size() / 2;
            Thread[] threadsMerge = new Thread[pares];
            byte[][] resultadosRodada = new byte[pares][];
            List<byte[]> proximaRodada = new ArrayList<>();

            for (int i = 0; i < pares; i++) {
                final int idx = i;
                byte[] v1 = listas.get(i * 2);
                byte[] v2 = listas.get(i * 2 + 1);

                threadsMerge[i] = new Thread(() -> {
                    resultadosRodada[idx] = MergeUtils.merge(v1, v2);
                });
                threadsMerge[i].start();
            }

            if (listas.size() % 2 != 0) {
                proximaRodada.add(listas.get(listas.size() - 1));
            }

            for (int i = 0; i < pares; i++) {
                threadsMerge[i].join();
                proximaRodada.add(0, resultadosRodada[i]);
            }
            listas = proximaRodada;
        }
        return listas.isEmpty() ? new byte[0] : listas.get(0);
    }
}