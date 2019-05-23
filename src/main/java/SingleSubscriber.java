import java.io.*;

public class SingleSubscriber {
    public static void main(String[] args) {

        /* 读入TXT文件 */
        String pathname = "data.txt";
        File filename = new File(pathname);
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(
                    new FileInputStream(filename));
            BufferedReader br = new BufferedReader(reader);
            String line = "";
            while ((line = br.readLine()) != null) {
                int index1 = line.indexOf(':');
                int index2 = line.indexOf('@');
                System.out.println(line.substring(index1 + 1, index2));
                System.out.println(Long.parseLong(line.substring(index2 + 1)));

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
