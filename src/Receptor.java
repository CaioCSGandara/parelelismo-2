import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Receptor {
    private static final int PORTA = 12345;

    public static void main(String[] args) {
        System.out.println("[R] Servidor Receptor ouvindo na porta " + PORTA);

        try (ServerSocket serverSocket = new ServerSocket(PORTA)) {
            while (true) {
                Socket cliente = serverSocket.accept();

                // Substitui a classe WorkerCliente por uma Thread com Lambda direto
                // Passamos a lógica de atendimento para um método estático auxiliar para não poluir o main
                new Thread(() -> tratarConexao(cliente)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Método que contém a lógica que antes estava em WorkerCliente.run()
    private static void tratarConexao(Socket socket) {
        try (
                ObjectOutputStream transmissor = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream receptor = new ObjectInputStream(socket.getInputStream())
        ) {
            boolean running = true;
            while (running) {
                Object obj = receptor.readObject();

                if (obj instanceof Pedido pedido) {
                    byte[] vetor = pedido.getNumeros();
                    System.out.println("[R] Pedido recebido. Tamanho: " + vetor.length);

                    // 1. DESCOBRIR PROCESSADORES
                    int numProcessadores = Runtime.getRuntime().availableProcessors();
                    System.out.println("[R] Usando " + numProcessadores + " threads para processar.");

                    // 2. DIVIDIR VETOR
                    List<byte[]> partesParaOrdenar = dividirVetor(vetor, numProcessadores);

                    // Estruturas para substituir a classe ThreadOrdenadora
                    Thread[] threads = new Thread[numProcessadores];
                    byte[][] resultadosOrdenacao = new byte[numProcessadores][];

                    // 3. CRIAR E INICIAR THREADS DE ORDENAÇÃO
                    for (int i = 0; i < numProcessadores; i++) {
                        // Se houver partes suficientes (caso vetor seja muito pequeno pode sobrar null)
                        if (i < partesParaOrdenar.size()) {
                            final int index = i;
                            byte[] parte = partesParaOrdenar.get(i);

                            threads[i] = new Thread(() -> {
                                // Escreve o resultado no array compartilhado
                                resultadosOrdenacao[index] = MergeUtils.sequentialMergeSort(parte);
                            });
                            threads[i].start();
                        }
                    }

                    // 4. JOIN (AGUARDAR TODAS)
                    List<byte[]> partesOrdenadas = new ArrayList<>();
                    for (int i = 0; i < numProcessadores; i++) {
                        if (threads[i] != null) {
                            threads[i].join(); // Bloqueia até terminar
                            partesOrdenadas.add(resultadosOrdenacao[i]);
                        }
                    }

                    // 5. MERGE FINAL PARALELO
                    byte[] resultadoFinal = executarMergeParalelo(partesOrdenadas);

                    transmissor.writeObject(new Resposta(resultadoFinal));
                    transmissor.flush();

                } else if (obj instanceof ComunicadoEncerramento) {
                    System.out.println("[R] Cliente enviou encerramento.");
                    running = false;
                }
            }
        } catch (Exception e) {
            System.err.println("[R] Conexão finalizada ou erro: " + e.getMessage());
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

    // Lógica refatorada para não usar ThreadJuntadora
    private static byte[] executarMergeParalelo(List<byte[]> listas) throws InterruptedException {
        while (listas.size() > 1) {
            int pares = listas.size() / 2;
            Thread[] threadsMerge = new Thread[pares];
            byte[][] resultadosRodada = new byte[pares][];
            List<byte[]> proximaRodada = new ArrayList<>();

            // Dispara threads para cada par
            for (int i = 0; i < pares; i++) {
                final int idx = i;
                byte[] v1 = listas.get(i * 2);
                byte[] v2 = listas.get(i * 2 + 1);

                threadsMerge[i] = new Thread(() -> {
                    resultadosRodada[idx] = MergeUtils.merge(v1, v2);
                });
                threadsMerge[i].start();
            }

            // Se sobrou vetor ímpar
            if (listas.size() % 2 != 0) {
                proximaRodada.add(listas.get(listas.size() - 1));
            }

            // Aguarda os merges
            for (int i = 0; i < pares; i++) {
                threadsMerge[i].join();
                proximaRodada.add(0, resultadosRodada[i]); // Adiciona no início
            }

            listas = proximaRodada;
        }
        return listas.isEmpty() ? new byte[0] : listas.get(0);
    }
}