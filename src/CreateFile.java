import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class CreateFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File fs = new File("/home/cloudera/Desktop/training.txt");
		FileWriter fw = new FileWriter(fs);
		int i=0;
		while(i <= 50){
			fw.write("my name is ram, i live in hicksville\n");
			i++;
		}
		fw.close();

	}

}
