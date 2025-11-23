public class Resposta extends Comunicado {
    private final byte[] numerosOrdenados;

    public Resposta(byte[] numerosOrdenados) {
        this.numerosOrdenados = numerosOrdenados;
    }

    public byte[] getNumerosOrdenados() {
        return numerosOrdenados;
    }
}