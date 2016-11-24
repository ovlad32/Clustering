package com.rokittech;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

public class ClusteringLauncher {
	// c --uid edm --pwd edmedm --url tcp://localhost:9092/edm --wid 41 --label
	// L1 --bl 0.4
	static Connection conn;

	static final String clusteredColumnTableDefinition = "create table if not exists link_clustered_column(\n"
			+ " column_info_id bigint not null \n" + " ,workflow_id   bigint not null \n"
			+ " ,cluster_no    integer not null \n" + " ,cluster_label varchar(100) not null \n"
			+ " ,constraint link_clustered_col_pk primary key (column_info_id, workflow_id, cluster_no, cluster_label)\n"
			+ ")";

	static final String clusteredColumnParamTableDefinition = "create table if not exists link_clustered_column_param(\n"
			+ " cluster_label varchar(100) not null \n" + " ,workflow_id   bigint not null \n"
			+ " ,bitset_level  real \n" + " ,lucene_level  real \n"
			+ " ,constraint link_clustered_col_par_pk primary key (workflow_id, cluster_label)\n" + ")";
   
	static final String columnGroupTableDefinintion = "create table if not exists link_column_group("+
			 "workflow_id  bigint not null , " +
			 "scope varchar(10) not null, " +
			 "parent_column_info_id bigint not null, " +
			 "child_column_info_id bigint not null," +
			 "group_num bigint,"+
			 "constraint link_column_group_pk primary key (parent_column_info_id,child_column_info_id,workflow_id,scope))";

	static final String initialClusteringQuery = "insert into link_clustered_column(column_info_id,workflow_id,cluster_no,cluster_label)\n"
			+ "              select \n" + "              t.parent_column_info_id as column_info_id \n"
			+ "    , t.workflow_id \n" + "    , t.cluster_no \n" + "    , t.cluster_label\n" + "    from (\n"
			+ "   select top 1 \n" + "          count(1) as cnt \n" + "          , p.* \n"
			+ "          , l.parent_column_info_id \n" + "     from link  l\n" + "      cross join (select\n"
			+ "         convert(?, varchar(100)) as cluster_label \n" + "         , convert(?, int)   as workflow_id \n"
			+ "         , convert(?, int)  as cluster_no \n" + "         , convert(?, real) as bitset_level \n"
			+ "         , convert(?, real) as lucene_level \n" + "         ) p\n"
			+ "      left outer join link_clustered_column c\n"
			+ "          on c.column_info_id in (l.parent_column_info_id,child_column_info_id)\n"
			+ "        and c.cluster_label = p.cluster_label\n" + "     where l.workflow_id = p.workflow_id\n"
			+ "     and (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null)\n"
			+ "     and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null)\n"
			+ "     and c.column_info_id is null\n" + "  group by parent_column_info_id\n" + "  having count(1) >1 \n"
			+ "  order by cnt desc\n" + ") t ";

	static final String workingClusteringQuery = "insert into link_clustered_column(column_info_id,workflow_id,cluster_no,cluster_label)\n"
			+ " select distinct t.column_info_id,t.workflow_id,t.cluster_no,t.cluster_label\n" + "  from (\n"
			+ "   select\n" + "     c.workflow_id\n" + "     , c.cluster_no\n" + "     , c.cluster_label\n"
			+ "     , case when c.column_info_id = l.child_column_info_id then l.parent_column_info_id else l.child_column_info_id end as column_info_id\n"
			+ "     from (select\n" + "         convert(?, varchar(100)) as cluster_label \n"
			+ "         , convert(?, int)   as workflow_id \n" + "         , convert(?, int)  as cluster_no \n"
			+ "         , convert(?, real) as bitset_level \n" + "         , convert(?, real) as lucene_level \n"
			+ "          ) p\n" + "   inner join link_clustered_column c\n" + "    on c.workflow_id = p.workflow_id\n"
			+ "   and c.cluster_no = p.cluster_no\n" + "   and c.cluster_label = p.cluster_label\n"
			+ "  inner join link l\n" + "   on l.workflow_id =  c.workflow_id\n"
			+ "  and c.column_info_id in (l.parent_column_info_id, l.child_column_info_id)\n"
			+ " where (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null)\n"
			+ "   and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null)\n"
			+ ") t left outer join link_clustered_column c\n" + "   on c.COLUMN_INFO_ID = t.column_info_id\n"
			+ "  and c.workflow_id  = t.workflow_id\n" + "  and c.cluster_label = t.cluster_label\n" +
			// " and c.cluster_no = t.cluster_no\n" +
			"  where c.column_info_id is null\n";

	static final String deleteClusteredColumn = "delete from link_clustered_column c\n" + " where c.workflow_id = ?\n"
			+ "   and c.cluster_label = ?";

	static final String deleteClusteredColumnParam = "delete from link_clustered_column_param c\n"
			+ " where c.workflow_id = ?\n" + "   and c.cluster_label = ?";

	static final String insertClusteredColumnParam = "insert into link_clustered_column_param(workflow_id,cluster_label,bitset_level,lucene_level) \n"
			+ " values(?,?,?,?)";

	static final String reportClusteredColumnsQuery = " select distinct\n" 
	        + "    c.cluster_no\n" 
			//+ " ,l.id,lr.id\n" 
			+ "     ,l.parent_db_name \n" 
			+ "     ,l.parent_schema_name \n" 
			+ "     ,l.parent_table_name \n"
			+ "     ,l.parent_name \n"  
			+ "     ,l.child_db_name \n" 
			+ "     ,l.child_schema_name \n"
			+ "     ,l.child_table_name \n" 
			+ "     ,l.child_name \n" 
			+ "     ,l.bit_set_exact_similarity \n"
			+ "     ,l.lucine_sample_term_similarity \n" 
			+ "     ,lr.bit_set_exact_similarity \n"
			+ "     ,lr.lucine_sample_term_similarity\n"  
			+ "     ,p.bitset_level\n"  
			+ "     ,p.lucene_level\n"  
			+ "   from link_clustered_column_param p\n"
			+ "     inner join link_clustered_column c on p.workflow_id = c.workflow_id and p.cluster_label = c.cluster_label\n"
			+ "     inner join link l on c.column_info_id in (l.parent_column_info_id,l.child_column_info_id)\n"
			+ "     left outer join link lr\n" + "       on lr.parent_column_info_id = l.child_column_info_id\n"
			+ "      and lr.child_column_info_id = l.parent_column_info_id\n"
			+ "      and lr.workflow_id = l.workflow_id\n" + "where p.workflow_id = ?\n" + " and p.cluster_label = ?\n"
			+ " and (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null)\n"
			+ " and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null)\n"
			+ " and (lr.id < l.id or lr.id is null)\n" + " order by 1 asc";

	
	private static final String deleteSameConfidenceColumnGroups =
			"delete from link_column_group where workflow_id = ? and scope='SAME_BS'";
	
	private static final String insertSameConfidenceColumnGroups =
	    "insert into link_column_group(workflow_id, scope, parent_column_info_id, child_column_info_id,group_num) "+
		"select l.workflow_id, "+
		"       'SAME_BS', "+
		"       l.parent_column_info_id, "+
		"       l.child_column_info_id, "+
		"       src.group_num "+
		" from link l  "+
		" inner join ( "+
		"     select rownum as group_num,z.*  from (select "+
		"               BIT_SET_EXACT_SIMILARITY, "+
		"               child_table_name, "+
		"               parent_table_name, "+
		"               child_schema_name, "+
		"               parent_schema_name, "+
		"               child_db_name, "+
		"               parent_db_name, "+
		"               workflow_id "+
		"         from link  "+
		"         where workflow_id = ? " +
		"           and BIT_SET_EXACT_SIMILARITY >0 " +
		"         group by  BIT_SET_EXACT_SIMILARITY, "+
		"                   child_table_name, "+
		"                   parent_table_name, "+
		"                   child_schema_name, "+
		"                   parent_schema_name, "+
		"                   child_db_name, "+
		"                   parent_db_name, "+
		"                   workflow_id "+
		"         having count(1)>1) z "+
		") src  "+
		"  on l.BIT_SET_EXACT_SIMILARITY = src.BIT_SET_EXACT_SIMILARITY "+
		"	and l.child_table_name = src.child_table_name "+
		"	and l.parent_table_name = src.parent_table_name "+
		"	and l.child_schema_name = src.child_schema_name "+
		"	and l.parent_schema_name = src.parent_schema_name "+
		"	and l.child_db_name = src.child_db_name "+
		"	and l.parent_db_name = src.parent_db_name "+
		"	and l.workflow_id = src.workflow_id ";


	static final String reportAllColumnPairsQuery = 
	 "select distinct " +
   	 "   l.parent_db_name " +
   	 "   ,l.parent_schema_name " +
   	 "   ,l.parent_table_name" +
   	 "   ,l.parent_name" +
   	 "   ,l.child_db_name" +
   	 "   ,l.child_schema_name" +
   	 "   ,l.child_table_name " +
   	 "   ,l.child_name " +
   	 "   ,l.BIT_SET_EXACT_SIMILARITY " +
   	 "   ,l.LUCINE_SAMPLE_TERM_SIMILARITY " +
   	 "   ,lr.BIT_SET_EXACT_SIMILARITY " +
   	 "   ,lr.LUCINE_SAMPLE_TERM_SIMILARITY " +
     "   ,bs.group_num " +
     "   ,case when cp.HASH_UNIQUE_COUNT = cc.HASH_UNIQUE_COUNT then 'x' end as unique_same" +  
     "   ,l.id " +  
     "   ,lr.id " +  
	  " from link l" +
	  " inner join column_info cp on cp.id = l.PARENT_COLUMN_INFO_ID" +
	  " inner join column_info cc on cc.id = l.CHILD_COLUMN_INFO_ID" +
	  " left outer join link_column_group bs" +
	  "   on bs.scope = 'SAME_BS'" +
	  "  and bs.workflow_id = l.workflow_id" +
	  "  and bs.parent_column_info_id = l.parent_column_info_id" + 
	  "  and bs.child_column_info_id = l.child_column_info_id " +
	  " left outer join link lr" +
	  "   on lr.workflow_id = l.workflow_id" +
	  "  and lr.child_column_info_id = l.parent_column_info_id" + 
	  "  and lr.parent_column_info_id = l.child_column_info_id " +
	  " where l.workflow_id = ?   " +
      "  and (lr.id<l.id or lr.id is null) "+
	  "  order by "+
	  "  l.parent_db_name,l.parent_schema_name,l.parent_table_name, " +
	  "  l.child_db_name,l.child_schema_name,l.child_table_name, group_num";


	private static void initH2(String url, String uid, String password) throws SQLException, RuntimeException,
			InstantiationException, IllegalAccessException, ClassNotFoundException {
		Driver driver = (Driver) Class.forName("org.h2.Driver").newInstance();

		Properties p = new Properties();

		if (url == null) {
			throw new RuntimeException("Error: URL to ASTRA H2 DB has not been specified!");
		}
		if (uid == null) {
			throw new RuntimeException("Error: User ID for ASTRA H2 DB has not been specified!");
		}
		if (password == null) {
			throw new RuntimeException("Error: Password for User ID of ASTRA H2 DB has not been specified!");
		}

		p.put("user", uid);
		p.put("password", password);
		conn = driver.connect("jdbc:h2:" + url, p);

		execSQL("SET AUTOCOMMIT OFF");

	}

	private static void execSQL(String command) throws SQLException {
		try (Statement st = conn.createStatement();) {
			st.execute(command);
		}
	}

	private static Float floatOf(String value) {
		if (value == null)
			return null;
		else
			return Float.valueOf(value);

	}

	private static Long longOf(String value) {
		if (value == null)
			return null;
		else
			return Long.valueOf(value);

	}

	public static void main(String[] args) {
		try {

			Properties parsedArgs = parseCommandLine(args);
			if (parsedArgs.size() == 0) {
				printHelp();
				return;
			}

			String command = parsedArgs.getProperty("command");
			if ("c".equals(command)) {
				initH2(parsedArgs.getProperty("url"), parsedArgs.getProperty("uid"), parsedArgs.getProperty("pwd"));

				execSQL(clusteredColumnTableDefinition);
				execSQL(clusteredColumnParamTableDefinition);

				deleteClusters(parsedArgs.getProperty("label"), longOf(parsedArgs.getProperty("wid")));

				createClusters(parsedArgs.getProperty("label"), longOf(parsedArgs.getProperty("wid")),
						floatOf(parsedArgs.getProperty("bl")), floatOf(parsedArgs.getProperty("ll")));
				if (parsedArgs.containsKey("outfile")) {
					reportClusters(parsedArgs.getProperty("label"), longOf(parsedArgs.getProperty("wid")),
							parsedArgs.getProperty("outfile"));
				}
				System.out.println("Done.");
			} else if ("a".equals(command)) {
				initH2(parsedArgs.getProperty("url"), parsedArgs.getProperty("uid"), parsedArgs.getProperty("pwd"));
				reportAllCoumnPairs(longOf(parsedArgs.getProperty("wid")),
						parsedArgs.getProperty("outfile"));
				System.out.println("Done.");
			} else if ("d".equals(command)) {
				initH2(parsedArgs.getProperty("url"), parsedArgs.getProperty("uid"), parsedArgs.getProperty("pwd"));
				deleteClusters(parsedArgs.getProperty("label"), longOf(parsedArgs.getProperty("wid")));
				System.out.println("Done.");
			} else if ("x".equals(command)) {
				initH2(parsedArgs.getProperty("url"), parsedArgs.getProperty("uid"), parsedArgs.getProperty("pwd"));
				reportClusters(parsedArgs.getProperty("label"), longOf(parsedArgs.getProperty("wid")),
						parsedArgs.getProperty("outfile"));
				System.out.println("Done.");
			} else if ("l".equals(command)) {
				initH2(parsedArgs.getProperty("url"), parsedArgs.getProperty("uid"), parsedArgs.getProperty("pwd"));
				reportLabels();
				//throw new RuntimeException("Not implemented yet");
			} else {

			}
		} catch (RuntimeException e) {
			System.err.print(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static boolean checkExistance(String args[], int index) {
		if (args.length < (index + 1) || args[index + 1].isEmpty() || args[index + 1].startsWith("--")) {
			System.out.printf(" parameter %v has no value !\n", args[index]);
			return false;
		}
		return true;
	}

	private static Properties parseCommandLine(String[] args) {
		Properties result = new Properties();

		if (args.length == 0) {
			return result;
		}

		if ("c".equals(args[0])) {
		} else if ("a".equals(args[0])) {
		} else if ("x".equals(args[0])) {
		} else if ("d".equals(args[0])) {
		} else if ("l".equals(args[0])) {
		} else {
			throw new RuntimeException(String.format("  %s - Invalid command!\n", args[0]));
		}

		result.put("command", args[0]);
		boolean ok = true;
		for (int index = 1; index < args.length; index = index + 2) {

			if ("--url".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--uid".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--pwd".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--label".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--outfile".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--wid".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					longOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have an integer value !\n", args[index]);
					ok = false;
				}
			} else if ("--bl".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					floatOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have a float value !\n", args[index]);
					ok = false;
				}
			} else if ("--ll".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					floatOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have a float value !\n", args[index]);
					ok = false;
				}
			} else {
				System.err.printf("parameter %s has not been recognized!\n",args[index]);
			}
		}

		if (!ok) {
			result.clear();
		}
		return result;

	}

	private static void printHelp() {
		System.out.println("Usage: Clustering.jar <command> <parameters>");
		System.out.println(" commands: ");
		System.out.println("   c : Create (and extract if parameter --outfile specified) column clusters");
		System.out.println("   a : extract All column pairs to output file");
		System.out.println("   x : eXtract clustered columns to output file");
		System.out.println("   d : Delete column clusters");
		System.out.println("   l : List of column cluster labels");
		System.out.println();
		System.out.println(" parameters: ");
		System.out.println("   --url  <string>     : URL to ASTRA H2 DB");
		System.out.println("   --uid <string>      : user id for ASTRA H2 DB");
		System.out.println("   --pwd  <string>     : password for ASTRA H2 DB");
		System.out.println("   --label <string>    : label to use while clustering");
		System.out.println("   --wid <integer>     : ASTRA workflow ID to process");
		System.out.println("   --bl <float>        : ASTRA bitset confidence level to process pairs");
		System.out.println("   --ll <float>        : ASTRA lucene confidence level to process pairs");
		System.out.println("   --outfile <string>  : output filename");
		System.out.println();
		System.out.println(" Examples:");
		System.out.println("   Clustering.jar c --url tcp://localhost:9092/edm --uid edm --pwd edmedm --wid 42 --label case1 --bl .7 --outfile result.xls");

	}
	
	
	
	static final String labelsQuery = 
	" select "+
	" workflow_id " +
    ", cluster_label " +
    ", bitset_level " +
    ", lucene_level " +
    "from link_clustered_column_param " +
    "order by 1,2 ";

	private static void reportLabels() throws SQLException {
		int counter = 0;
		try (PreparedStatement ps = conn.prepareStatement(labelsQuery);
		     ResultSet rs = ps.executeQuery()) {
			 while (rs.next()) {
				 counter++;
				 if (counter == 1) {
					 System.out.println("Stored labels:");
					 System.out.println("|---|----------|--------------------|------------|------------|");
					 System.out.println("| # |WorkflowID|Label name          |Lucene level|Bitset level|");
					 System.out.println("|---|----------|--------------------|------------|------------|");
				 }
				 
				 System.out.printf("|%1$3d|%2$10d|%3$-20s|%4$12.5f|%5$12.5f|\n",
						 counter,
						 rs.getObject(1),
						 rs.getObject(2),
						 rs.getObject(3),
						 rs.getObject(4)
						 );
			 }
			 
			 if (counter>0) {
				 System.out.println("|---|----------|--------------------|------------|------------|");
			 } else {
				 System.out.println("No Labels");
			 }
			 
			
			
		}
	}
	private static void deleteClusters(String clusterLabel, Long workflowId) throws SQLException {
		if (clusterLabel == null || clusterLabel.isEmpty()) {
			throw new RuntimeException("Error: Cluster Label has not been specified!");
		}

		if (workflowId == null) {
			throw new RuntimeException("Error: Workflow ID has not been specified!");
		}

		try (PreparedStatement psCC = conn.prepareStatement(deleteClusteredColumn);
				PreparedStatement psCCP = conn.prepareStatement(deleteClusteredColumnParam);) {
			psCC.setLong(1, workflowId);
			psCC.setString(2, clusterLabel);
			psCC.executeUpdate();
			psCCP.setLong(1, workflowId);
			psCCP.setString(2, clusterLabel);
			psCCP.executeUpdate();
		}
		conn.commit();
	}

	private static void createClusters(String clusterLabel, Long workflowId, Float bitsetLevel, Float luceneLevel)
			throws SQLException {
		long clusterNo = 0;
		int updated = 0;

		if (clusterLabel == null || clusterLabel.isEmpty()) {
			throw new RuntimeException("Error: Cluster Label has not been specified!");
		}

		if (workflowId == null) {
			throw new RuntimeException("Error: Workflow ID has not been specified!");
		}

		if (bitsetLevel == null && luceneLevel == null) {
			throw new RuntimeException("Error: Neither Bitset nor Lucene confidence level has not been specified!");
		}

		try (PreparedStatement mainPS = conn.prepareStatement(initialClusteringQuery);
				PreparedStatement workingPS = conn.prepareStatement(workingClusteringQuery);
				PreparedStatement insertParamPS = conn.prepareStatement(insertClusteredColumnParam);) {
			for (;;) {
				clusterNo++;

				mainPS.setString(1, clusterLabel);
				mainPS.setObject(2, workflowId);
				mainPS.setLong(3, clusterNo);
				mainPS.setObject(4, bitsetLevel);
				mainPS.setObject(5, luceneLevel);
				updated = mainPS.executeUpdate();
				if (updated == 0) {
					conn.rollback();

					insertParamPS.setLong(1, workflowId);
					insertParamPS.setString(2, clusterLabel);
					insertParamPS.setObject(3, bitsetLevel);
					insertParamPS.setObject(4, luceneLevel);
					insertParamPS.executeUpdate();
					conn.commit();

					break;
				}

				for (;;) {
					workingPS.setString(1, clusterLabel);
					workingPS.setObject(2, workflowId);
					workingPS.setLong(3, clusterNo);
					workingPS.setObject(4, bitsetLevel);
					workingPS.setObject(5, luceneLevel);
					updated = workingPS.executeUpdate();

					if (updated < 2) {
						conn.rollback();
						break;
					}
					conn.commit();
				}
			}
		}
		
		System.out.printf("Clusters have been successfuly created.\nNumber of clusters %d\n",clusterNo);

		conn.commit();
	}

	private static void reportClusters(String clusterLabel, Long workflowId, String outFile)
			throws SQLException, IOException {
		Locale.setDefault(Locale.US);
		if (clusterLabel == null || clusterLabel.isEmpty()) {
			throw new RuntimeException("Error: Cluster Label has not been specified!");
		}

		if (workflowId == null) {
			throw new RuntimeException("Error: Workflow ID has not been specified!");
		}

		if (outFile == null || outFile.isEmpty()) {
			throw new RuntimeException("Error: Output file has not been specified!");
		}

		int rowCount = 0;
		try (PreparedStatement st = conn.prepareStatement(reportClusteredColumnsQuery);) {
			st.setLong(1, workflowId);
			st.setString(2, clusterLabel);

			try (ResultSet rs = st.executeQuery(); 
					HTMLFileWriter out = new HTMLFileWriter(outFile)) {
				while (rs.next()) {
					rowCount++;

					if (rowCount == 1) {
						out.write("<HTML>");
						out.write("<HEADER>");
						out.write("<meta http-equiv=Content-Type content='text/html; charset=UTF-8'>");
						out.write("<STYLE>");
						out.write(".confidence {mso-number-format:\"0\\.00000\";text-align:right;}");
						out.write("</STYLE>");
						out.write("</HEADER>");
						out.write("<BODY>");
						out.write("<P style='font-weight:bold;'>");
						out.write("Workflow ID: "); out.text(String.valueOf(workflowId));
						out.write("; Label: ");		out.text(clusterLabel);
						out.write("; Bitset Confidence Level: ");		out.textf("%f",rs.getObject(14));
						out.write("; Lucene Confidence Level: ");		out.textf("%f",rs.getObject(15));
						out.write(";</P>");
						out.write("<TABLE BORDER>");
						out.write("<col width=50>"+
						 "<col width=100>"+
						 "<col width=128>"+
						 "<col width=150>"+
						 "<col width=175>"+
						 "<col width=100>"+
						 "<col width=120>"+
						 "<col width=150>"+
						 "<col width=175>"+
						 "<col width=100>"+
						 "<col width=100 >"+
						 "<col width=100>"+
						 "<col width=100>");
						
						out.write("<TR height=49 width=61 style='height:36.75pt;width:46pt'>");
						out.element("TH", "Cluster #");
						out.element("TH", "Parent DB name");
						out.element("TH", "Parent schema name");
						out.element("TH", "Parent table name");
						out.element("TH", "Parent column name");
						out.element("TH", "Child DB name");
						out.element("TH", "Child schema name");
						out.element("TH", "Child table name");
						out.element("TH", "Child column name");

						out.element("TH", "Bitset confidence");
						out.element("TH", "Lucene confidence");
						out.element("TH", "Reversal Bitset confidence");
						out.element("TH", "Reversal Lucene confidence");

						out.write("</TR>");
					}
					out.write("<TR>");

					// cluster_no
					out.elementf("TD", "%d", rs.getLong(1));

					// parent_db_name
					out.element("TD", rs.getString(2));

					// parent_schema_name
					out.element("TD", rs.getString(3));

					// parent_table_name
					out.element("TD", rs.getString(4));

					// parent_column_name
					out.element("TD", rs.getString(5));

					// child_db_name
					out.element("TD", rs.getString(6));

					// child_schema_name
					out.element("TD", rs.getString(7));

					// child_table_name
					out.element("TD", rs.getString(8));

					// child_column_name
					out.element("TD", rs.getString(9));

					// bit_set_exact_similarity
					out.elementf("TD","class='confidence'", "%f", rs.getFloat(10));

					// lucine_sample_term_similarity
					out.elementf("TD","class='confidence'", "%f"	, rs.getFloat(11));

					// rev_bit_set_exact_similarity
					out.elementf("TD","class='confidence'", "%f", rs.getObject(12));

					// rev_lucine_sample_term_similarity
					out.elementf("TD","class='confidence'", "%f", rs.getObject(13));

					out.write("</TR>");

				}
				out.write("</TABLE>");
				out.write("</BODY>");
				out.write("</HTML>");

			}

		}
		System.out.printf("Report has been successfuly written to file %s \n",outFile);
	}
    
	private static void reportAllCoumnPairs(Long workflowId, String outFile) 
		throws SQLException, IOException {
			Locale.setDefault(Locale.US);

			if (workflowId == null) {
				throw new RuntimeException("Error: Workflow ID has not been specified!");
			}

			if (outFile == null || outFile.isEmpty()) {
				throw new RuntimeException("Error: Output file has not been specified!");
			}
			execSQL(columnGroupTableDefinintion);
			
			try(PreparedStatement ps = conn.prepareStatement(deleteSameConfidenceColumnGroups)){
				ps.setLong(1, workflowId);
				ps.executeUpdate();
				conn.commit();
			}
			
			try(PreparedStatement ps = conn.prepareStatement(insertSameConfidenceColumnGroups)){
				ps.setLong(1, workflowId);
				ps.executeUpdate();
				conn.commit();
			}
			
			try(PreparedStatement ps = conn.prepareStatement(reportAllColumnPairsQuery)) {
				ps.setLong(1, workflowId);
				int counter = 0;
				try (ResultSet rs = ps.executeQuery();
						HTMLFileWriter out = new HTMLFileWriter(outFile)) {
					while(rs.next()) {
						counter++;
						if (counter == 1) {
							out.write("<HTML>");
							out.write("<HEADER>");
							out.write("<meta http-equiv=Content-Type content='text/html; charset=UTF-8'>");
							out.write("<STYLE>");
							out.write(".confidence {mso-number-format:\"0\\.00000\";text-align:right;}");
							out.write(".integer {mso-number-format:\"0\";text-align:right;}");
							out.write("</STYLE>");
							out.write("</HEADER>");
							out.write("<BODY>");
							out.write("<P style='font-weight:bold;'> Workflow ID: ");
							out.write(String.valueOf(workflowId));
							out.write("</P>");
							out.write("<TABLE BORDER>");
							out.write(""+
							 "<col width=100>"+
							 "<col width=128>"+
							 "<col width=150>"+
							 "<col width=175>"+
							 "<col width=100>"+
							 "<col width=120>"+
							 "<col width=150>"+
							 "<col width=175>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=50>"+
							 "<col width=50>"+
							 "");
							
							out.write("<TR height=49 width=61 style='height:36.75pt;width:46pt'>");
							out.element("TH", "Parent DB name");
							out.element("TH", "Parent schema name");
							out.element("TH", "Parent table name");
							out.element("TH", "Parent column name");
							out.element("TH", "Child DB name");
							out.element("TH", "Child schema name");
							out.element("TH", "Child table name");
							out.element("TH", "Child column name");

							out.element("TH", "Bitset confidence");
							out.element("TH", "Lucene confidence");
							out.element("TH", "Reversal Bitset confidence");
							out.element("TH", "Reversal Lucene confidence");
							
							out.element("TH", "Bitset group");
							out.element("TH", "Distinct count");
							
							out.element("TH", "Link ID");
							out.element("TH", "Reversal Link ID");

							out.write("</TR>");
						}
						
						out.write("<TR>");

						// parent_db_name
						out.element("TD", rs.getString(1));

						// parent_schema_name
						out.element("TD", rs.getString(2));

						// parent_table_name
						out.element("TD", rs.getString(3));

						// parent_column_name
						out.element("TD", rs.getString(4));

						// child_db_name
						out.element("TD", rs.getString(5));

						// child_schema_name
						out.element("TD", rs.getString(6));

						// child_table_name
						out.element("TD", rs.getString(7));

						// child_column_name
						out.element("TD", rs.getString(8));

						// bit_set_exact_similarity
						out.elementf("TD","class='confidence'", "%f", rs.getObject(9));

						// lucine_sample_term_similarity
						out.elementf("TD","class='confidence'", "%f"	, rs.getObject(10));

						// rev_bit_set_exact_similarity
						out.elementf("TD","class='confidence'", "%f", rs.getObject(11));

						// rev_lucine_sample_term_similarity
						out.elementf("TD","class='confidence'", "%f", rs.getObject(12));


						// Bitset group
						out.elementf("TD","class='integer'", "%d", rs.getObject(13));

						// Distict count
						out.element("TD", "style='text-align:center;'",rs.getString(14));

						//Link Id
						out.elementf("TD","class='integer'", "%d", rs.getObject(15));
						//Reversal Link Id
						out.elementf("TD","class='integer'", "%d", rs.getObject(16));

						out.write("</TR>");

					}
					out.write("</TABLE>");
					out.write("</BODY>");
					out.write("</HTML>");
				}
			}
			System.out.printf("Report has been successfuly written to file %s \n",outFile);
	}
	
	
	
	
	static class HTMLFileWriter extends FileWriter {

		static final String nbsp = "&nbsp;";
		
		public HTMLFileWriter(String fileName) throws IOException {
			super(fileName);
		}

		public void text(String str) throws IOException {
			if (str == null || str.isEmpty()) {
				super.write(nbsp);
			} else {
				super.write(str);
			}
		}

		public void textf(String format, Object str) throws IOException {
			if (str == null)
				super.write(nbsp);
			else if (str instanceof String && ((String) str).isEmpty()) 
				super.write(nbsp);
			else 
				super.write(String.format(format, str));
		}

		public void element(String tag, String str) throws IOException {
			super.write("<"); super.write(tag);super.write(">");
			this.text(str);
			super.write("</"); super.write(tag);super.write(">");
		}

		public void element(String tag, String attrs, String str) throws IOException {
			super.write("<"); super.write(tag);super.write(" "); super.write(attrs); super.write(">");
			this.text(str);
			super.write("</"); super.write(tag);super.write(">");
		}

		public void elementf(String tag, String format, Object str) throws IOException {
			super.write("<"); super.write(tag);super.write(">");
			this.textf(format, str);
			super.write("</"); super.write(tag);super.write(">");
		}

		public void elementf(String tag, String attrs, String format, Object str) throws IOException {
			super.write("<"); super.write(tag); super.write(" "); super.write(attrs); super.write(">");
			this.textf(format, str);
			super.write("</"); super.write(tag);super.write(">");
		}

	}
}
