import java.util.Random;

public class OrdenacaoLocal {
    public static void main(String[] args) {
        try {
            System.out.println("--- ORDENAÇÃO LOCAL (SEM REDE) ---");
            int tamanhoTotal = 30_000_000; // Mesmo tamanho do distribuído
            byte[] vetor = new byte[tamanhoTotal];
            new Random().nextBytes(vetor);

            System.out.println("Iniciando ordenação local...");
            long inicio = System.currentTimeMillis();

            // Usa o mesmo helper, mas rodando tudo numa máquina só
            byte[] resultado = MergeSortHelper.parallelSort(vetor);

            long fim = System.currentTimeMillis();
            System.out.println("Tempo Local: " + (fim - inicio) / 1000.0 + " segundos");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}