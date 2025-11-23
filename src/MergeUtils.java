public class MergeUtils {

    // Merge Sort Sequencial (usado pelas threads individuais nas folhas)
    public static byte[] sequentialMergeSort(byte[] array) {
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

    // Junta dois vetores ordenados em um só
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