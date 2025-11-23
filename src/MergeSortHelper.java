import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MergeSortHelper {

    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Ponto de entrada para ordenar um vetor usando paralelismo local.
     * Segue a regra: Threads ordenadoras -> Threads juntadoras (2 a 2).
     */
    public static byte[] parallelSort(byte[] input) throws InterruptedException, ExecutionException {
        if (input.length < 1000) {
            // Se for muito pequeno, ordena direto na thread atual
            return sequentialMergeSort(input);
        }

        ExecutorService executor = Executors.newFixedThreadPool(CORES);
        try {
            // 1. FASE DE DIVISÃO E ORDENAÇÃO (Threads Ordenadoras)
            // Divide o vetor em N partes (onde N = número de processadores)
            List<Future<byte[]>> futures = new ArrayList<>();
            int chunkSize = input.length / CORES;
            int remainder = input.length % CORES;

            int start = 0;
            for (int i = 0; i < CORES; i++) {
                int size = chunkSize + (i < remainder ? 1 : 0); // Distribui o resto
                if (size == 0) continue;

                int finalStart = start;
                int finalSize = size;

                // Cria thread para ordenar este pedaço
                futures.add(executor.submit(() -> {
                    byte[] part = new byte[finalSize];
                    System.arraycopy(input, finalStart, part, 0, finalSize);
                    return sequentialMergeSort(part);
                }));

                start += size;
            }

            // Coleta os pedaços ordenados
            List<byte[]> chunks = new ArrayList<>();
            for (Future<byte[]> future : futures) {
                chunks.add(future.get());
            }

            // 2. FASE DE JUNÇÃO (Threads Juntadoras)
            // Junta os vetores 2 a 2 até sobrar apenas 1
            return parallelMerge(chunks, executor);

        } finally {
            executor.shutdown();
        }
    }

    /**
     * Junta uma lista de vetores ordenados em um único vetor ordenado,
     * utilizando threads para fazer merge 2 a 2.
     */
    public static byte[] parallelMerge(List<byte[]> chunks, ExecutorService executor)
            throws InterruptedException, ExecutionException {

        while (chunks.size() > 1) {
            List<Future<byte[]>> mergeFutures = new ArrayList<>();

            // Pega pares de vetores e submete para threads de merge
            for (int i = 0; i < chunks.size(); i += 2) {
                byte[] left = chunks.get(i);
                if (i + 1 < chunks.size()) {
                    byte[] right = chunks.get(i + 1);
                    mergeFutures.add(executor.submit(() -> merge(left, right)));
                } else {
                    // Sobrou um vetor ímpar, passa para a próxima rodada
                    byte[] solo = left;
                    mergeFutures.add(executor.submit(() -> solo));
                }
            }

            // Prepara a lista para a próxima rodada
            chunks = new ArrayList<>();
            for (Future<byte[]> f : mergeFutures) {
                chunks.add(f.get());
            }
        }

        return chunks.isEmpty() ? new byte[0] : chunks.get(0);
    }

    // Merge Sort Sequencial (Recursivo padrão)
    private static byte[] sequentialMergeSort(byte[] array) {
        if (array.length <= 1) return array;

        int mid = array.length / 2;
        byte[] left = new byte[mid];
        byte[] right = new byte[array.length - mid];

        System.arraycopy(array, 0, left, 0, left.length);
        System.arraycopy(array, mid, right, 0, right.length);

        left = sequentialMergeSort(left);
        right = sequentialMergeSort(right);

        return merge(left, right);
    }

    // Merge de dois vetores ordenados (Complexidade O(n))
    public static byte[] merge(byte[] left, byte[] right) {
        byte[] result = new byte[left.length + right.length];
        int i = 0, j = 0, k = 0;

        while (i < left.length && j < right.length) {
            if (left[i] <= right[j]) {
                result[k++] = left[i++];
            } else {
                result[k++] = right[j++];
            }
        }

        while (i < left.length) result[k++] = left[i++];
        while (j < right.length) result[k++] = right[j++];

        return result;
    }
}