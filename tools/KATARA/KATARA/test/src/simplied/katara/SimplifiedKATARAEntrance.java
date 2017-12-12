package simplied.katara;
import java.io.*;
import java.util.*;
import qa.qcri.katara.dbcommon.Table;
import qa.qcri.katara.kbcommon.KBReader;
import qa.qcri.katara.kbcommon.KnowledgeDatabaseConfig;
import qa.qcri.katara.kbcommon.PatternDiscoveryException;


public class SimplifiedKATARAEntrance {

	public static void main(String[] args) throws Exception {
		
		
		//setup

		//String rdb = "/home/milad/Desktop/abstraction-layer/tools/KATARA/country4.csv";

		Scanner scanner = new Scanner(System.in);
		String rdb = scanner.next();
		//System.out.println(rdb);

		//String rdb = "country4.csv";


		String kb="./tools/KATARA/mykb";
		String output_errors_file = rdb + "_katara_errors.txt";
		

		//String domainSpecificKB = "/home/milad/Desktop/abstraction-layer/tools/KATARA/dominSpecific";

		Scanner scanner1 = new Scanner(System.in);
		String domainSpecificKB = scanner1.next();
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
