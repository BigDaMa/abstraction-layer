package simplied.katara;
import java.io.*;
import java.util.*;
import qa.qcri.katara.dbcommon.Table;
import qa.qcri.katara.kbcommon.KBReader;
import qa.qcri.katara.kbcommon.KnowledgeDatabaseConfig;
import qa.qcri.katara.kbcommon.PatternDiscoveryException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class SimplifiedKATARAEntrance {

	public static void main(String[] args) throws Exception {


		//setup

		//String rdb = "/home/milad/Desktop/abstraction-layer/tools/KATARA/country4.csv";

		Scanner scanner = new Scanner(System.in);
		String rdb = scanner.next();
		//System.out.println(rdb);

		//String rdb = "country4.csv";


		//String kb="/home/milad/Desktop/Untitled Folder/abstraction-layer/tools/KATARA/mykb";
		Path path=Paths.get("");
		String myKbPath[]=path.toAbsolutePath().toString().split("/");
		String kb="";
		for (int i=0;i<myKbPath.length-1;i++){
			kb+=myKbPath[i]+"/";
		}
		kb+="abstraction-layer/tools/KATARA/mykb";

		String output_errors_file = rdb + "_katara_errors.txt";


		//String domainSpecificKB = "/home/milad/Desktop/abstraction-layer/tools/KATARA/dominSpecific";

		//Scanner scanner1 = new Scanner(System.in);
		String domainSpecificKB = scanner.next();
		//System.out.println(domainSpecificKB);

		//run KATARA
		KnowledgeDatabaseConfig.setDataDirectoryBase(kb);
		KnowledgeDatabaseConfig.KBStatsDirectoryBase = kb + "Stats";
		KnowledgeDatabaseConfig.frequentPercentage = 0.5;

		//Let's only deal with the first 1000 rows
		Table table = new Table(rdb,Integer.MAX_VALUE);

		KBReader reader = new KBReader();
		SimplifiedPatternDiscovery spd = new SimplifiedPatternDiscovery(table,reader,domainSpecificKB);
		spd.errorDetection(false);
		spd.print_errors(output_errors_file);
		reader.close();

	}



}
