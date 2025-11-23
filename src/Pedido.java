public class Pedido extends Comunicado {
    private final byte[] numeros;

    public Pedido(byte[] numeros) {
        this.numeros = numeros;
    }

    public byte[] getNumeros() {
        return numeros;
    }
}