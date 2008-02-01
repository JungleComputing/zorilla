package nl.vu.zorilla.apps;

public final class Sleep {

    /**
     * 
     * @param args
     */
    public static void main(String[] args) {

        System.out.println("sleeping for " + args[0] + " seconds");

        try {
            Thread.sleep(Integer.parseInt(args[0]) * 1000);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
