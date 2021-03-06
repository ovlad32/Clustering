package com.rokittech;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.security.cert.CollectionCertStoreParameters;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.zaxxer.sparsebits.SparseBitSet;


public class ClusteringLauncher {
	// Clustering.jar a --url tcp://52.59.69.151:9090/./data/h2/edm --uid edm --pwd edmedm --wid 57 --label L2 --bl 0.1 --outfile res.html
	//c --url tcp://52.59.69.151:9090/./data/h2/edm --uid edm --pwd edmedm --wid 162 --label L129 --bl 0.1 --bucket 1000 --outfile res.html
	// c --url tcp://52.29.37.253:9090/./data/h2/edm --uid edm --pwd edmedm --wid 290 --label LabelTest --bl 0.01 --outfile res.html
	//c --url tcp://52.29.37.253:9090/./data/h2/edm --uid edm --pwd edmedm --wid 225 --label LabelTest --bl 0.01
	static Connection conn;
	static boolean workingTableTemporary = false;
/*
 
			select * from link_clustered_column where workflow_id = 66
			delete from link_clustered_column where workflow_id = 66

			select pci.min_val,pci.max_val,cci.id,cci.min_val,cci.max_val from link l
 inner join column_info cci on cci.id = l.child_column_info_id
 inner join column_info pci on pci.id = l.parent_column_info_id
where workflow_id = 66 and parent_column_info_id = 947


*/
	
	private static final String clusteredColumnTableDefinition = "create table if not exists link_clustered_column(\n"
			+ " column_info_id bigint not null \n" 
			+ " ,workflow_id   bigint not null \n"
			+ " ,cluster_number    integer not null \n" 
			+ " ,cluster_type    char(1)\n" 
			+ " ,cluster_label varchar(100) not null \n"
			+ " ,processing_order bigint"
			+ " ,pass_number bigint"
			+ " ,leading_column_info_id bigint"
			+ " ,leading_unique_count_lowerbound double"
			+ " ,leading_unique_count_upperbound double"
			+ " ,leading_total_count_lowerbound double"
			+ " ,leading_total_count_upperbound double"
			+ " ,in_unique_count boolean"
			+ " ,in_total_count boolean"
			+ " ,constraint link_clustered_col_pk primary key (column_info_id, workflow_id, cluster_number, cluster_label)\n"
			+ ")";

	private static final String clusteredColumnParamTableDefinition = "create table if not exists link_clustered_column_param(\n"
			+ " cluster_label varchar(100) not null \n" + " ,workflow_id   bigint not null \n"
			+ " ,bitset_level  real \n" + " ,lucene_level  real \n"
			+ " ,constraint link_clustered_col_par_pk primary key (workflow_id, cluster_label)\n" + ")";
   
	private static final String columnGroupTableDefinintion = "create table if not exists link_column_group("+
			 "workflow_id  bigint not null , " +
			 "scope varchar(10) not null, " +
			 "parent_column_info_id bigint not null, " +
			 "child_column_info_id bigint not null," +
			 "group_num bigint,"+
			 "constraint link_column_group_pk primary key (parent_column_info_id,child_column_info_id,workflow_id,scope))";

	private static final String deleteClusteredColumn = "delete from link_clustered_column c\n" + " where c.workflow_id = ?\n"
			+ "   and c.cluster_label = ?";

	private static final String deleteClusteredColumnParam = "delete from link_clustered_column_param c\n"
			+ " where c.workflow_id = ?\n" + "   and c.cluster_label = ?";

	
	private static final String reportClusteredColumnsQuery = " select distinct\n" 
	        + "    c.cluster_number \n" 
			//+ " ,l.id,lr.id\n" 
			+ "     ,l.parent_db_name \n" 
			+ "     ,l.parent_schema_name \n" 
			+ "     ,l.parent_table_name \n"
			+ "     ,l.parent_name  as parent_column_name\n"  
			+ "     ,l.child_db_name \n" 
			+ "     ,l.child_schema_name \n"
			+ "     ,l.child_table_name \n" 
			+ "     ,l.child_name  as child_column_name\n" 
			+ "     ,l.bit_set_exact_similarity  as BS_CONFIDENCE\n"
			+ "     ,l.lucine_sample_term_similarity as LC_CONFIDENCE \n" 
			+ "     ,lr.bit_set_exact_similarity as REV_BS_CONFIDENCE\n"
			+ "     ,lr.lucine_sample_term_similarity  as REV_LC_CONFIDENCE\n "  
			+ "     ,p.bitset_level\n"  
			+ "     ,p.lucene_level\n"  
			+ "     ,pc.real_type         as parent_real_type \n"
			+ "     ,pc.hash_unique_count as parent_huq \n"
			+ "     ,pc.min_fval as parent_min \n"  
			+ "     ,pc.max_fval as parent_max \n"  
		    + "     ,null as parent_is_sequence "
			+ "     ,pc.std_dev          as parent_std_dev \n" 
			+ "     ,pc.moving_mean      as parent_moving_mean \n"  
			+ "     ,cast(null as double)  as parent_median \n" 
		    + "     ,cc.real_type         as child_real_type \n"  
			+ "     ,cc.hash_unique_count as child_huq \n"
			+ "     ,cc.min_fval as child_min \n"  
			+ "     ,cc.max_fval as child_max \n"  
		    + "     ,null as child_is_sequence "
			+ "     ,cc.moving_mean      as child_moving_mean \n" 
			+ "     ,cc.std_dev          as child_std_dev \n" 
			+ "     ,cast(null as double) as child_median \n"  
			+ "   from link_clustered_column_param p\n"
			+ "     inner join link_clustered_column c on p.workflow_id = c.workflow_id and p.cluster_label = c.cluster_label\n"
			+ "     inner join link l on c.column_info_id in (l.parent_column_info_id,l.child_column_info_id)\n"
			+ "     inner join column_info pc on pc.id = l.parent_column_info_id "
			+ "     inner join column_info cc on cc.id = l.child_column_info_id "
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


	private static final String reportAllColumnPairsQuery = 
	 "select distinct " +
   	 "   l.parent_db_name " +
   	 "   ,l.parent_schema_name " +
   	 "   ,l.parent_table_name" +
   	 "   ,l.parent_name as parent_column_name" +
   	 "   ,l.child_db_name" +
   	 "   ,l.child_schema_name" +
   	 "   ,l.child_table_name " +
   	 "   ,l.child_name as child_column_name" +
   	 "   ,l.BIT_SET_EXACT_SIMILARITY  as bs_confidence" +
   	 "   ,l.LUCINE_SAMPLE_TERM_SIMILARITY as lc_confidence" +
   	 "   ,lr.BIT_SET_EXACT_SIMILARITY as rev_bs_confidence" +
   	 "   ,lr.LUCINE_SAMPLE_TERM_SIMILARITY as rev_lc_confidence" +
     "   ,bs.group_num as bitset_group_num" +
     "   ,case when pc.HASH_UNIQUE_COUNT = cc.HASH_UNIQUE_COUNT then 'Y' end as unique_same" +  
	 "   ,pc.real_type         as parent_real_type \n" +
	 "   ,pc.hash_unique_count as parent_huq \n" + 
	 "   ,pc.data_scale as parent_data_scale \n" + 
	 "   ,pc.min_fval   as parent_min \n" + 
	 "   ,pc.max_fval   as parent_max \n" + 
     "   ,null as parent_is_sequence "+
	 "   ,pc.std_dev          as parent_std_dev \n" + 
	 "   ,pc.moving_mean      as parent_moving_mean \n" + 
	 "   ,cast(null as double) as parent_median \n" + 
	 "   ,pc.position_in_pk   as parent_position_in_constraint \n"+
	 "   ,pc.total_in_pk      as parent_total_columns_in_pk\n"+
     "   ,cc.real_type         as child_real_type \n" + 
	 "   ,cc.hash_unique_count as child_huq \n"+ 
	 "   ,cc.data_scale as child_data_scale \n" + 
	 "   ,cc.min_fval as child_min \n" + 
	 "   ,cc.max_fval as child_max \n" + 
     "   ,null  as child_is_sequence "+
	 "   ,cc.moving_mean      as child_moving_mean \n" + 
	 "   ,cc.std_dev          as child_std_dev \n" + 
	 "   ,cc.median           as child_median \n" + 
	 "   ,cc.position_in_pk   as child_position_in_constraint \n"+
	 "   ,cc.total_in_pk      as child_total_columns_in_pk\n"+
	 "   ,case greatest(pc.max_fval, cc.max_fval) "+
	 "              - least(pc.min_fval, cc.min_fval) <> 0 then "+
	 "          1.0*(abs(pc.min_fval - cc.min_fval) + abs(pcs.max_fval - ccs.max_fval)) / "+
	 "            (greatest(pc.max_fval, cc.max_fval) - "+
	 "              - least(pc.min_fval, cc.min_fval) ) end as range_similarity" +
	 "   ,(select top 1 'buckets'||b1.column_id from column_numeric_bucket b1 where b1.column_id = l.parent_column_info_id) as parent_buckets \n"+
	 "	 ,(select top 1 'buckets'||b1.column_id from column_numeric_bucket b1 where b1.column_id = l.child_column_info_id) as child_buckets \n"+
	 "   ,l.parent_column_info_id \n" +
	 "   ,cc.table_info_id as child_table_info_id \n"+
	 "   ,l.child_column_info_id \n" + 
	 "   ,pc.table_info_id as parent_table_info_id \n"+
	 "   ,case when pc.position_in_pk is not null and (\n"+
     "	        select top 1 'Y' from link l1 \n"+
     "	        inner join column_info cc1 on cc1.id = l1.child_column_info_id \n"+
     "	        inner join column_info pc1 on pc1.id = l1.parent_column_info_id \n"+
     "	        where l1.workflow_id = l.workflow_id \n"+
     "	         and cc1.table_info_id = cc.table_info_id \n"+
     "	         and pc1.table_info_id = pc.table_info_id \n"+
     "	         and pc1.id <> l.parent_column_info_id) is null then 'Y' end as parent_pk_only_pair \n"+
	 "   ,case when cc.position_in_pk is not null and (\n"+
     "	        select top 1 'Y' from link l1 \n"+
     "	        inner join column_info cc1 on cc1.id = l1.child_column_info_id \n"+
     "	        inner join column_info pc1 on pc1.id = l1.parent_column_info_id \n"+
     "	        where l1.workflow_id = l.workflow_id \n"+
     "	         and cc1.table_info_id = cc.table_info_id \n"+
     "	         and pc1.table_info_id = pc.table_info_id \n"+
     "	         and cc1.id <> l.child_column_info_id) is null then 'Y' end as child_pk_only_pair \n"+
	 "   ,l.id as link_id" +  
     "   ,lr.id as rev_link_id " +    
	  " from link l" +
	  " inner join column_info pc on pc.id = l.parent_column_info_id " +
	  " inner join column_info cc on cc.id = l.child_column_info_id " +
	  " left outer join link_column_group bs" +
	  "   on bs.scope = 'SAME_BS'" +
	  "  and bs.workflow_id = l.workflow_id" +
	  "  and bs.parent_column_info_id = l.parent_column_info_id " + 
	  "  and bs.child_column_info_id = l.child_column_info_id " +
	  " left outer join link lr" +
	  "   on lr.workflow_id = l.workflow_id" +
	  "  and lr.child_column_info_id = l.parent_column_info_id " + 
	  "  and lr.parent_column_info_id = l.child_column_info_id " +
	  " where l.workflow_id = ?   " +
      "  and (lr.id<l.id or lr.id is null) "+
	  "  order by "+
	  "  l.parent_db_name,l.parent_schema_name,l.parent_table_name, " +
	  "  l.child_db_name,l.child_schema_name,l.child_table_name, bitset_group_num";

    
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
		
		execSQL(columnGroupTableDefinintion);
		
		makeTableColStats();
		makeTableNumericRealType();


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

	/*
	public static void p() throws SQLException {
		Driver d = new org.postgresql.Driver();
		Properties props = new Properties();
		props.setProperty("user", "gpadmin");
		props.setProperty("password", "pivotal");
		String sql = "select " +
				"   t.table_schema   as SCHEMA_NAME " +
				"   ,t.table_catalog as DATABASE_NAME " +
				"   ,t.table_name    as NAME " +
				"   ,'TABLE'::text         as TYPE " +
				"   ,null::timestamp as CREATED " +
				"   ,null::timestamp as LAST_DDL_TIME " +
				"   ,0               as KB " +
				"  from information_schema.tables as t " +
				"where t.table_type = 'BASE TABLE' " +
				"   and t.table_schema = ? ";
		try(Connection c = d.connect("jdbc:postgresql://10.200.80.143:5432/postgres", props)){
			try (PreparedStatement ps = c.prepareStatement(sql)){
				ps.setString(1, "cra");
				try(ResultSet rs = ps.executeQuery()){
					while (rs.next()) {
						for (int i = 1;i<=6; i++)
							System.out.println(rs.getObject(i));
					}
				}
			}
		}

	}
	*/
	public static void main(String[] args) throws SQLException {
		

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

				
				createClustersV3(
						longOf(parsedArgs.getProperty("wid")),
						parsedArgs.getProperty("label"),
						parsedArgs.getProperty("diffdb"),
						parsedArgs.getProperty("contenttype"),
						parsedArgs.getProperty("linktable"),
						floatOf(parsedArgs.getProperty("bl")), 
						floatOf(parsedArgs.getProperty("ll")),
						floatOf(parsedArgs.getProperty("rl")),
						floatOf(parsedArgs.getProperty("ts")),
						floatOf(parsedArgs.getProperty("ucs")),
						floatOf(parsedArgs.getProperty("tcs"))
						);
				
				
				
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
			} else if ("s".equals(command)) {
				initH2(parsedArgs.getProperty("url"), parsedArgs.getProperty("uid"), parsedArgs.getProperty("pwd"));
				/*List<String> params = new ArrayList<>();
				params.add("IS_SEQ");
				params.add("MOVING_MEAN");
				if (parsedArgs.getProperty("bucket") != null) {
					params.add("BUCKETS");
				}
				pairStatistics(longOf(parsedArgs.getProperty("wid")), params,parsedArgs);
				*/
				List<Long> ids = getTableIdList(longOf(parsedArgs.getProperty("wid")),longOf(parsedArgs.getProperty("mid")),longOf(parsedArgs.getProperty("tid")));
				for (Long id:ids) {
					calculateDumpStats(id,parsedArgs.getProperty("basedir","./"));;
				}
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
		} else if ("s".equals(args[0])) {
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
			} else if ("--basedir".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--diffdb".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--linktable".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--contenttype".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				result.put(args[index].substring(2), args[index + 1]);
			} else if ("--wid".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					longOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %s does not have an integer value !\n", args[index]);
					ok = false;
				}
			} else if ("--mid".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					longOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %s does not have an integer value !\n", args[index]);
					ok = false;
				}			} else if ("--tid".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					longOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %s does not have an integer value !\n", args[index]);
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
			} else if ("--rl".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					floatOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have a float value !\n", args[index]);
					ok = false;
				}
			} else if ("--ts".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					floatOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have a float value !\n", args[index]);
					ok = false;
				}
			} else if ("--ucs".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					floatOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have a float value !\n", args[index]);
					ok = false;
				}
			} else if ("--tcs".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					floatOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have a float value !\n", args[index]);
					ok = false;
				}
			} else if ("--bucket".equals(args[index])) {
				ok = ok || checkExistance(args, index);
				try {
					longOf(args[index + 1]);
					result.put(args[index].substring(2), args[index + 1]);
				} catch (NumberFormatException e) {
					System.err.printf(" parameter %v does not have a integer value !\n", args[index]);
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
		System.out.println("   s : Caluclating statistics");
		System.out.println();
		System.out.println(" parameters: ");
		System.out.println("   --url  <string>      : URL to ASTRA H2 DB");
		System.out.println("   --uid <string>       : user id for ASTRA H2 DB");
		System.out.println("   --pwd  <string>      : password for ASTRA H2 DB");
		System.out.println("   --wid <integer>      : ASTRA workflow ID to process");
		System.out.println("   --mid <integer>      : ASTRA metadata ID to process (stats)");
		System.out.println("   --tid <integer>      : ASTRA table ID to process (stats)");
		System.out.println("   --label <string>     : Label name for clustering");
		System.out.println("   --bl <float>         : ASTRA Bitset confidence level of column pairs to be clustered");
		System.out.println("   --ll <float>         : ASTRA Lucene confidence level of column pairs to be clustered");
		System.out.println("   --rl <float>         : Limit of initial pair range reduction");
		System.out.println("   --ts <float>         : Sweep of top values of columns to be clustered");
		System.out.println("   --ucs <float>        : Sweep of unique count of values of columns to be clustered");
		System.out.println("   --tcs <float>        : Sweep of total count of values of columns to be clustered");
		System.out.println("   --diffdb <Y/N>       : Distinguish columns by database");
		System.out.println("   --linktable <string> : Alternative Astra LINK table name  ");
		System.out.println("   --contenttype <A/N/S>: Content type to cluster:A-All;N-Numeric;S-String");
		System.out.println("   --bucket <integer>   : Calculating data buckets with width of <integer>");
		System.out.println("   --outfile <string>   : Output file name");
		System.out.println("   --basedir <string>   : Astra base directory");
		System.out.println();
		System.out.println(" Examples:");
		System.out.println();
		System.out.println(" - to create clusters:");
		System.out.println(" Clustering.jar c --url tcp://localhost:9092/edm --uid edm --pwd edmedm --wid 42 --label case1 --bl .7 --ll .3 --outfile result.xls");
		System.out.println();
		System.out.println(" - to export clustered pairs:");
		System.out.println(" Clustering.jar x --url tcp://localhost:9092/edm --uid edm --pwd edmedm --wid 42 --label case1 --outfile clusteredPairs.xls");
		System.out.println();
		System.out.println(" - to calculate statistics:");
		System.out.println(" Clustering.jar s --url tcp://localhost:9092/edm --uid edm --pwd edmedm --wid 42 -bucket 1000");
		System.out.println();
		System.out.println(" - to export all pairs:");
		System.out.println(" Clustering.jar a --url tcp://localhost:9092/edm --uid edm --pwd edmedm --wid 42 --outfile allPairs.xls");

	}
	
	
	


	private static void reportLabels() throws SQLException {
		int counter = 0;
		
		try (PreparedStatement ps = conn.prepareStatement(
				" select "+
				" workflow_id " +
			    ", cluster_label " +
			    ", bitset_level " +
			    ", lucene_level " +
			    "from link_clustered_column_param " +
			    "order by 1,2 ");
		     ResultSet rs = ps.executeQuery()) {
			 while (rs.next()) {
				 counter++;
				 if (counter == 1) {
					 System.out.println("Stored labels:");
					 System.out.println("|---|----------|--------------------|------------|------------|");
					 System.out.println("| # |WorkflowID|Label name          |Bitset level|Lucene level|");
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



	private static String getWorkingTableModifierString() {
		return workingTableTemporary ? " memory local temporary " : "";
	}
	

 public static List<Long> getTableIdList(Long workflowId,Long metadataId,Long tableId) throws SQLException {
	 List<Long> result = new ArrayList<>();
	 if (workflowId == null && metadataId == null && tableId == null) {
		 throw new RuntimeException("One of parameters: table id (-tid) or metadata id (-mid) or workflow id (-wid) has to be defined ");
	 }
	 if (workflowId != null) {
		 try(PreparedStatement ps = conn.prepareStatement(
		 "select distinct t.id from link l "
		 + "  inner join column_info c on c.id in (l.parent_column_info_id,l.child_column_info_id)"
		 + "  inner join table_info t on t.id = c.table_info_id "
		 + " where workflow_id = ?")){
			 ps.setLong(1, workflowId);
			 try(ResultSet rs = ps.executeQuery()) {
				 while(rs.next())
					 result.add(rs.getLong(1));
			 }
		 }
	 } else if (metadataId != null) {
		 try(PreparedStatement ps = conn.prepareStatement(
			 "select t.id from table_info t "
			 + "  where t.metadata_id = ?")){
			 ps.setLong(1, metadataId);
			 try(ResultSet rs = ps.executeQuery()) {
				 while(rs.next())
					 result.add(rs.getLong(1));
			 }
		}
		 
	 }else if (tableId != null) {
		 result.add(tableId);
	 }
	 return result;
 }


public static class NCBoundaries {
	private BigDecimal lower, upper, initialRange;
}

public static class NCColumn {
	private Long id;
	private Long dbId;
	private BigDecimal minValue;
	private BigDecimal maxValue;
	private BigDecimal uniqueCount,totalCount;
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NCColumn other = (NCColumn) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	public NCColumn(Long id, Long dbId, 
			BigDecimal minValue, 
			BigDecimal maxValue
			) {
		if (id == null) {
			throw new NullPointerException("ColumnId is null!");
		}
		if (dbId == null) {
			throw new NullPointerException("ColumnDbId is null!");
		}
		if (minValue == null) {
			throw new NullPointerException(String.format("MinValue is null for Column Id = %d!",id));
		}
		if (maxValue == null) {
			throw new NullPointerException(String.format("MaxValue is null for Column Id = %d!",id));
		}

		this.id = id;
		this.dbId = dbId;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.uniqueCount = uniqueCount;
		this.totalCount = totalCount;
	}
		
	
	
	/*NCColumn(Long id) throws SQLException {
		if (id == null) {
			throw new NullPointerException("ColumnId is null!");
		}
		
		try (PreparedStatement ps = conn.prepareStatement(
				"select m.database_config_id, c.min_val, c.max_val "
				+ " from column_info_numeric_range_view c "
				+ "  inner join table_info t on t.id = c.table_info_id "
				+ "  inner join metadata on m.id = t.metadata_id "
				+ " where c.id = ?")){
				ps.setLong(1, id);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						init(id, rs.getLong(1), rs.getBigDecimal(2), rs.getBigDecimal(3));
					} else {
						throw new RuntimeException(String.format("Column Id %d has not been found", id));
					}
				}
			}
	} */
	
	void saveToQueue() throws SQLException{
		try(PreparedStatement ps = conn.prepareStatement(
				"merge into t$queue(column_id,column_db_id,min_val,max_val) "
				+ " key(column_id) "
				+ " values(?,?,?,?)")){
			ps.setLong(1, this.id);
			ps.setLong(2, this.dbId);
			ps.setBigDecimal(3, this.minValue);
			ps.setBigDecimal(4, this.maxValue);
			ps.executeUpdate();
		}
	}
	
	boolean saveAsClustered(Long clusterNumber,long processingOrder, long passNumber,NCBoundaries boundaries,Long leadingId ) {
		boolean result;
		try(PreparedStatement psu = conn.prepareStatement(
				"merge into t$column("
				+ "cluster_type"
				+ ",column_id"
				+ ",cluster_number"
				+ ",processing_order"
				+ ",pass_number"
				+ ",leading_column_id"
				+ ",initial_range"
				+ ",lower_bound"
				+ ",upper_bound"
				+ ") key (column_id,cluster_number) "
				+" values('N',?,?,?,?,?,?,?,?)")) {
		int index =0;
		psu.setLong(++index, this.id);
		psu.setLong(++index, clusterNumber);
		psu.setLong(++index, processingOrder);
		psu.setLong(++index, passNumber);
		psu.setObject(++index, leadingId);
		psu.setBigDecimal(++index, boundaries.initialRange);
		psu.setBigDecimal(++index, boundaries.lower);
		psu.setBigDecimal(++index, boundaries.upper);
		result = psu.executeUpdate()>0; 
		} catch(SQLException e) {
			throw new RuntimeException(String.format("Exception while saving clustered column column_id=%d",this.id),e);
		}
		
		try (PreparedStatement psd = conn.prepareStatement(
				"update t$queue set excluded = true where column_id = ?")){
			psd.setLong(1, this.id);
			psd.executeUpdate();
		} catch(SQLException e) {
			throw new RuntimeException(String.format("Exception while removing clustered column from queue column_id=%d",this.id),e);
		}
		
			
		return result;
	}
		
	boolean isFit(NCBoundaries boundaries) {
		return this.minValue.compareTo(boundaries.upper) <= 0 && 
				this.maxValue.compareTo(boundaries.lower) >= 0 ;
	}
	
	void shorten(NCBoundaries boundaries) {
		if(this.minValue.compareTo(boundaries.lower) > 0 ) boundaries.lower = this.minValue;
		if(this.maxValue.compareTo(boundaries.upper) < 0 ) boundaries.upper = this.maxValue;
	}

}




static void acInitializeWorkingTables(
			Long workflowId,
			String clusterLabel,
			String diffDb,
			String contentType,
			Float bitsetLevel,
			Float luceneLevel,
			Float rangeLimit,
			Float topSweep,
			Float uniqueCountSweep,
			Float totalCountSweep
			) throws SQLException{
	
	
	
	if (clusterLabel == null || clusterLabel.isEmpty()) {
		throw new RuntimeException("Error: Cluster Label has not been specified!");
	}

	if (workflowId == null) {
		throw new RuntimeException("Error: Workflow ID has not been specified!");
	}

	if (bitsetLevel == null && luceneLevel == null) {
		throw new RuntimeException("Error: Neither Bitset nor Lucene confidence level has not been specified!");
	}

	execSQL("drop table if exists t$column");
	execSQL("drop table if exists t$queue");
	execSQL("drop table if exists t$param");
	execSQL("drop table if exists s$column");
	
	try(PreparedStatement ps = conn.prepareStatement(
			"create  "+getWorkingTableModifierString()+" table t$param as "
			+ "select "
			+ "  cast(? as bigint) workflow_id "
			+ ", cast(? as varchar(100)) as cluster_label "
			+ ", cast(? as char(1)) as diff_db"
			+ ", cast(? as char(1)) as content_type"
			+ ", cast(? as decimal(10,5)) as bitset_level "
			+ ", cast(? as decimal(10,5)) as lucene_level "
			+ ", cast(? as decimal(10,5)) as range_limit "
			+ ", cast(? as decimal(10,5)) as top_sweep "
			+ ", cast(? as decimal(10,5)) as unique_count_sweep "
			+ ", cast(? as decimal(10,5)) as total_count_sweep "
			+ " "
			)){
		int index = 0;  
		ps.setLong(++index, workflowId);
		ps.setString(++index, clusterLabel);
		ps.setObject(++index, diffDb);
		ps.setObject(++index, contentType);
		ps.setObject(++index, bitsetLevel);
		ps.setObject(++index, luceneLevel);
		ps.setObject(++index, rangeLimit);
		ps.setObject(++index, topSweep);
		ps.setObject(++index, uniqueCountSweep);
		ps.setObject(++index, totalCountSweep);
		ps.execute();
	};

	execSQL("create  "+getWorkingTableModifierString()+" table t$queue ("
			+ "column_id bigint primary key "
			+ ",column_db_id bigint "
			+ ",min_val double "
			+ ",max_val double "
			+ ",unique_count bigint "
			+ ",total_count bigint "
			+ ",excluded boolean default false)");

	execSQL("create  "+getWorkingTableModifierString()+" table t$column ("
			+ "column_id bigint "
			+ ",unique_count bigint"
			+ ",total_count bigint"
			+ ",cluster_type char(1)"
			+ ",cluster_number bigint "
			+ ",processing_order bigint "
			+ ",pass_number bigint "
			+ ",leading_column_id bigint"
			+ ",initial_range double "
			+ ",lower_bound double "
			+ ",upper_bound double "
			+ ",constraint t$column_pk primary key(column_id,cluster_number))");

	
	execSQL("create  "+getWorkingTableModifierString()+" table s$column ("
			+ "column_id bigint "
			+ ",column_db_id bigint "
			+ ",unique_count bigint"
			+ ",total_count bigint"
			+ ",cluster_type char(1)"
			+ ",cluster_number bigint "
			+ ",processing_order bigint "
			+ ",pass_number bigint "
			+ ",leading_column_id bigint"
			+ ",constraint s$column_pk primary key(column_id,cluster_number))");
}

	private static void ncPopulateLinkTable(String altLinkTable) throws SQLException {
		
		execSQL("drop table if exists t$link");
		
		execSQL("create "+getWorkingTableModifierString()+" table t$link as "
						+ " select "
						+ "   l.id  as link_id  "
						+ "   ,l.bit_set_exact_similarity as bitset_level  "
						+ "   ,l.lucine_sample_term_similarity as  lucene_level  "
						+ "   ,l.parent_column_info_id as parent_id  "
						+ "   ,l.child_column_info_id as child_id  "
						+ "   ,mpi.database_config_id as parent_db_id "
						+ "   ,mci.database_config_id as child_db_id "
						+ "   ,pi.min_val as parent_min_val "
						+ "   ,pi.max_val as parent_max_val "
						+ "   ,ci.min_val as child_min_val "
						+ "   ,ci.max_val as child_max_val "
						+ "	  ,greatest(pi.min_val, ci.min_val) as link_min "
						+ "	  ,least(pi.max_val,ci.max_val) as link_max "
						+ "   ,pi.unique_count as parent_unique_count"
						+ "   ,ci.unique_count as child_unique_count"
						+ "   ,pi.total_count as parent_total_count"
						+ "   ,ci.total_count as child_total_count"
						+ "	 from t$param p"
						+ "      inner join "+altLinkTable+" l on l.workflow_id = p.workflow_id "
						+ "	     inner join column_info_numeric_range_view ci on ci.id = l.child_column_info_id "
						+ "      inner join table_info tci on tci.id = ci.table_info_id "
						+ "      inner join metadata mci on mci.id = tci.metadata_id "
						+ "	     inner join column_info_numeric_range_view pi on pi.id = l.parent_column_info_id "
						+ "      inner join table_info tpi on tpi.id = pi.table_info_id "
						+ "      inner join metadata mpi on mpi.id = tpi.metadata_id "
						+ "	   where p.content_type in ('A','N')"
						+ "      and (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null)"
						+ "      and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null) "
						+ "      and pi.min_val is not null "
						+ "      and pi.max_val is not null "
						+ "      and ci.min_val is not null "
						+ "      and ci.max_val is not null "
						+ "      and ci.has_numeric_content = true "
						+ "      and pi.has_numeric_content = true ");
						
		
	
		execSQL("create hash index t$link_parent_id on t$link(parent_id)");
		execSQL("create hash index t$link_child_id on t$link(child_id)");
		
	
	}
	
	private static void scPopulateLinkTable(String altLinkTable) throws SQLException {
			
			execSQL("drop table if exists s$link");
			
			execSQL("create "+getWorkingTableModifierString()+" table s$link as "
					+ "  select  "
					+ "   l.id  as link_id "
					+ "   ,l.bit_set_exact_similarity as bitset_level "
					+ "   ,l.lucine_sample_term_similarity as  lucene_level "
					+ "   ,l.parent_column_info_id as parent_id "
					+ "   ,l.child_column_info_id as child_id "
					+ "   ,mpi.database_config_id as parent_db_id"
					+ "   ,mci.database_config_id as child_db_id"
					+ "   ,nvl(pi.unique_row_count,pi.hash_unique_count) as parent_unique_count"
					+ "   ,nvl(ci.unique_row_count,ci.hash_unique_count) as child_unique_count"
					+ "   ,pi.total_row_count as parent_total_count"
					+ "   ,ci.total_row_count as child_total_count"
					+ "	 from t$param p"
					+ "      inner join "+altLinkTable+" l on l.workflow_id = p.workflow_id "
					+ "	     inner join column_info ci on ci.id = l.child_column_info_id "
					+ "      inner join table_info tci on tci.id = ci.table_info_id "
					+ "      inner join metadata mci on mci.id = tci.metadata_id "
					+ "	     inner join column_info pi on pi.id = l.parent_column_info_id "
					+ "      inner join table_info tpi on tpi.id = pi.table_info_id "
					+ "      inner join metadata mpi on mpi.id = tpi.metadata_id "
					+ "	   where p.content_type in ('A','S') "
					+ "      and (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null) "
					+ "      and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null) "
					//+ "      and not exists (select 'Y' from t$column tc where tc.column_id = l.parent_column_info_id) "
					//+ "      and not exists (select 'Y' from t$column tc where tc.column_id = l.child_column_info_id) "
					+ "      and nvl(ci.has_numeric_content,false) = false "
					+ "      and nvl(pi.has_numeric_content,false) = false ");
			
			execSQL("create hash index s$link_parent_id on s$link(parent_id)");
			execSQL("create hash index s$link_child_id on s$link(child_id)");
			
	}



	private static void nc$populateLinkedColumns(NCColumn column) throws SQLException{
		try (PreparedStatement ps = conn.prepareStatement(
				"select 'Y' from t$queue where column_id = ?")
				){
			ps.setLong(1,column.id);
			try(ResultSet rs = ps.executeQuery()) { 
				// column was added to the queue in the previous iteration 
				if (rs.next()) {
					execSQL("update t$queue set excluded = false");
					return;
				} else {
					// if not, erase everything and populate again
					execSQL("truncate table t$queue");
					column.saveToQueue();
			  }
			}
		}
		
		while(true) {
			try(PreparedStatement ps = conn.prepareStatement(
						" insert into t$queue(column_id, column_db_id, min_val, max_val, unique_count, total_count) "
						+ " select c.id, t.column_db_id, c.min_val,c.max_val, c.unique_count, c.total_count "
						+ " from ( "
						+ " select "
						+ "  t.child_id  as column_id"
						+ "  ,t.child_db_id as column_db_id "
						+ "  from t$queue h "
						+ "   inner join t$link t  on t.parent_id = h.column_id"
						+ " union "
						+ " select "
						+ "  t.parent_id as column_id"
						+ "  ,t.parent_db_id as column_db_id "
						+ "  from t$queue h "
						+ "   inner join t$link t on t.child_id = h.column_id"
						+ " minus "
						+ " select column_id, column_db_id from t$queue"
						+ ") t inner join column_info_numeric_range_view c on c.id = t.column_id ")) {
				
				int count = ps.executeUpdate();
				if ( count == 0) break;
			}
		}
	}
	

	private static NCColumn nc$fetchMostUbiquitousColumn() throws SQLException {
		//TODO: PAIRS > 1 !!!
		
		try(Statement ps = conn.createStatement();
					ResultSet rs = ps.executeQuery(
					" select top 1 column_id, column_db_id,min_val, max_val, range_val, pairs,unique_count,total_count "
					+ " from ("
					+ "   select l.parent_id as column_id  "
					+ "    ,l.parent_db_id as column_db_id  "
					+ "    ,count(*) as  pairs "
					+ "    ,l.parent_min_val as min_val "
					+ "    ,l.parent_max_val as max_val "
					+ "    ,l.parent_max_val - l.parent_min_val as range_val"
					+ "    ,l.parent_unique_count as unique_count "
					+ "    ,l.parent_total_count as total_count "
					+ "  from t$link l "
					+ "  cross join t$param p "
					+ "  left outer join t$column cp "
					+ "     on cp.column_id = l.parent_id "
					+ "  where cp.column_id is null "
					+ "    and 'Y' = case "
					+ "          when p.top_sweep is null then 'Y' "
					+ "          when l.parent_max_val = l.child_max_val then 'Y' "
					+ "          when 'Y' = case "
					+ "						  when greatest(abs(l.parent_max_val - l.parent_min_val),abs(l.child_max_val - l.child_min_val)) > 0 then 'Y' "
					+ "                     end"
					+ "           then "
					+ "             case when abs(l.parent_max_val - l.child_max_val) / "
					+ "                  greatest(abs(l.parent_max_val - l.parent_min_val),abs(l.child_max_val - l.child_min_val))  < p.top_sweep "
					+ "                  then 'Y' end"
					+ "           end "
					+ "    group by l.parent_id,l.parent_db_id,l.parent_min_val, l.parent_max_val"
					+ "       ,l.parent_unique_count,l.parent_total_count "
					+ "    having pairs > 0"
					+ "  union "
					+ "	 select l.child_id as column_id "
					+ "    ,l.child_db_id as column_db_id  "
					+ "    ,count(*) as  pairs "
					+ "    ,l.child_min_val as min_val "
					+ "    ,l.child_max_val as max_val "
					+ "    ,l.child_max_val - l.child_min_val as range_val "
					+ "    ,l.child_unique_count as unique_count "
					+ "    ,l.child_total_count as total_count "
					+ "  from t$link l "
					+ "  cross join t$param p "
					+ "  left outer join t$column cc "
					+ "    on cc.column_id = l.child_id "
					+ "  where cc.column_id is null"
					+ "    and 'Y' = case "
					+ "          when p.top_sweep is null then 'Y' "
					+ "          when l.parent_max_val = l.child_max_val then 'Y' "
					+ "          when 'Y' = case "
					+ "						  when greatest(abs(l.parent_max_val - l.parent_min_val),abs(l.child_max_val - l.child_min_val)) > 0 then 'Y' "
					+ "                     end"
					+ "           then "
					+ "             case when abs(l.parent_max_val - l.child_max_val) / "
					+ "                  greatest(abs(l.parent_max_val - l.parent_min_val),abs(l.child_max_val - l.child_min_val))  < p.top_sweep "
					+ "                  then 'Y' end"
					+ "           end "
					+ "   group by l.child_id,l.child_db_id,l.child_min_val, l.child_max_val "
					+ "       ,l.child_unique_count,l.child_total_count "
					+ "   having pairs > 0"
					+ ")  order by range_val desc, pairs desc"
					)){
			if (!rs.next()) 
				return null;
			else {
				NCColumn result = new NCColumn(
						rs.getLong("column_id"),
						rs.getLong("column_db_id"),
						rs.getBigDecimal("min_val"),
						rs.getBigDecimal("max_val")
						);
				result.uniqueCount= rs.getBigDecimal("unique_count");
				result.totalCount = rs.getBigDecimal("total_count");
				return result;
			}
		}
	}





	private static void createClustersV3(
			Long workflowId, 
			String clusterLabel, 
			String diffDb,
			String contentType,
			String altLinkTable,
			Float bitsetLevel, 
			Float luceneLevel,
			Float rangeLimitFloat,
			Float topSweep,
			Float uniqueCountSweep,
			Float totalCountSweep
			) throws SQLException {

		long clusterNumber = 0;
		long passNumber = 0;
		BigDecimal rangeLimit = new BigDecimal(rangeLimitFloat.floatValue());
		rangeLimit = rangeLimit.setScale(5, RoundingMode.FLOOR);
		
		BigDecimal one = (new BigDecimal(1)).setScale(5, RoundingMode.FLOOR);
		
		BigDecimal backwardRangeLimit = one.add(one.subtract(rangeLimit)); 
		rangeLimit = rangeLimit.setScale(5, RoundingMode.FLOOR);
		
		if (diffDb == null || diffDb.trim().isEmpty()) {
			diffDb = "N";
		}
		if (altLinkTable == null || altLinkTable.trim().isEmpty()) {
			altLinkTable = "link";
		}
		if (contentType == null || contentType.isEmpty() ){
			contentType = "A";
		}
		
		conn.setAutoCommit(true);
	
		
		
		acInitializeWorkingTables(workflowId,clusterLabel,diffDb.toUpperCase().trim(),contentType,
				bitsetLevel,luceneLevel,rangeLimit.floatValue(),topSweep,
				uniqueCountSweep,
				totalCountSweep);
	
		ncPopulateLinkTable(altLinkTable);
		
		NCColumn leadingColumn = null;
		while (true) {
			leadingColumn = nc$fetchMostUbiquitousColumn();
			if (leadingColumn == null) {
				break;
			}
			 
			nc$populateLinkedColumns(leadingColumn);
		
			NCBoundaries clusterBoundaries = new NCBoundaries();
			clusterBoundaries.lower = leadingColumn.minValue;
			clusterBoundaries.upper = leadingColumn.maxValue;
			clusterBoundaries.initialRange = leadingColumn.maxValue.subtract(leadingColumn.minValue);
			long processingOrder = 0;
			leadingColumn.saveAsClustered( ++clusterNumber, ++processingOrder, ++passNumber, clusterBoundaries,null);
			try(PreparedStatement pst = conn.prepareStatement(
							" select "
							+ "  t.column_id "
							+ "  ,t.column_db_id "
							+ "  ,cc.name "
							
							//+ "  ,least(q.boundary_max_val, t.max_val)  "
							//+ "    - greatest(q.boundary_min_val, t.min_val) as range_val "
							
							+ "  ,t.max_val - t.min_val as range_val "
							+ "  ,t.min_val "
							+ "  ,t.max_val "
							+ " from t$queue t "
							+ "  cross join (select "
							+ "   cast(? as double) as boundary_min_val"
							+ "   ,cast(? as double) as boundary_max_val"
							+ "   ,cast(? as double) as leading_column_db_id"
							+ "   ,cast(? as double) as top_min_val"
							+ "   ,cast(? as double) as top_max_val"
							+ "   ,cast(? as bigint) as leading_unique_count"
							+ "   ,cast(? as bigint) as leading_total_count"
							+ "  ) q "
							+ " cross join t$param p "
							+ "  inner join column_info cc on cc.id = t.column_id"
							+ "  left outer join t$column c "
							+ "     on c.column_id = t.column_id "
							+ " where t.excluded = false "
							+ "    and (q.leading_column_db_id <> t.column_db_id or p.diff_db = 'Y')"
						    + "    and (p.unique_count_sweep is null or "
						 	+ "         t.unique_count between (q.leading_unique_count - q.leading_unique_count*p.unique_count_sweep) "
						 	+ "                            and (q.leading_unique_count + q.leading_unique_count*p.unique_count_sweep) ) "
						 	+ "    and (p.total_count_sweep is null  or "
						 	+ "         t.total_count between (q.leading_total_count - q.leading_total_count*p.total_count_sweep) "
						 	+ "                           and (q.leading_total_count + q.leading_total_count*p.total_count_sweep) ) "
							+ "    and c.column_id is null"
							+ "    and 'Y' = case "
							+ "        when p.top_sweep is null then 'Y' "
							+ "        when t.max_val = q.top_max_val then 'Y' "
							+ "        when 'Y' = case when greatest(abs(t.max_val - t.min_val),abs(q.top_max_val - q.top_min_val)) then 'Y' end "
							+ "         then case when abs(t.max_val - q.top_max_val) / "
							+ "          greatest(abs(t.max_val - t.min_val),abs(q.top_max_val - q.top_min_val)) < p.top_sweep "
							+ "          then 'Y' end"
							+ "        end"
							+ " order by c.column_id nulls first, range_val desc "))	{
				int index = 0;
				pst.setBigDecimal(++index, clusterBoundaries.lower);
				pst.setBigDecimal(++index, clusterBoundaries.upper);
				pst.setLong(++index, leadingColumn.dbId);
				pst.setBigDecimal(++index, leadingColumn.minValue);
				pst.setBigDecimal(++index, leadingColumn.minValue);
				pst.setBigDecimal(++index, leadingColumn.uniqueCount);
				pst.setBigDecimal(++index, leadingColumn.totalCount);
				try (ResultSet rs = pst.executeQuery()){ 
					while(rs.next()) {
						BigDecimal currentRange = rs.getBigDecimal("range_val");
						BigDecimal currentMin = rs.getBigDecimal("min_val"); 
						BigDecimal currentMax = rs.getBigDecimal("max_val");

						if (currentRange == null || clusterBoundaries.initialRange == null) 
								continue;
						if (clusterBoundaries.initialRange.compareTo(BigDecimal.ZERO) == 0) {
							if (leadingColumn.minValue.compareTo(currentMin) != 0 || 
									leadingColumn.maxValue.compareTo(currentMax) != 0) 
									continue;
						} else {
							BigDecimal result = currentRange.divide(
									clusterBoundaries.initialRange,
									5,
									RoundingMode.FLOOR
									);
							if (result.compareTo(rangeLimit) < 0){ 
								continue; 
							} else if (result.compareTo(backwardRangeLimit)>0) {
								continue; 
							}	
						}
						
						NCColumn column = new NCColumn(
									rs.getLong("column_id"),
									rs.getLong("column_db_id"),
									rs.getBigDecimal("min_val"),
									rs.getBigDecimal("max_val")
								);
								
						 //!!Here is the place where transitive link filter happens
						if (column.isFit(clusterBoundaries)) {
							column.shorten(clusterBoundaries);
							column.saveAsClustered(clusterNumber, ++processingOrder, passNumber, clusterBoundaries,leadingColumn.id);
						}
					}
				}
			}
				
				
		}
		
		
		
		
		scPopulateLinkTable(altLinkTable);
		
		
		String initialQuery =
				 "		    select "
				+ "		         count(*) as pairs "  
				+ "		        ,l.parent_id as column_id"
				+ "             ,l.parent_db_id as column_db_id"
				+ "		      from s$link  l "
				+ "			   inner join s$link lb "
				+ "                on lb.parent_id = l.child_id "
				+ "               and lb.child_id = l.parent_id "
				+ "		       left outer join s$column sc "
				+ "		           on sc.column_id  =  l.parent_id "
				+ "		       left outer join t$column tc "
				+ "		           on tc.column_id  =  l.parent_id "
				+ "		      where sc.column_id is null "
				+ "             and tc.column_id is null " 
				+ "		     group by l.parent_id,l.parent_db_id"
				+ "		     having count(*) >0  "
				+ "         union"
				+ "		    select "
				+ "		         count(*) as pairs "  
				+ "		        ,l.child_id as column_id"
				+ "             ,l.child_db_id as column_db_id"
				+ "		      from s$link  l  "
				+ "			   inner join s$link lb "
				+ "                on lb.parent_id = l.child_id "
				+ "               and lb.child_id = l.parent_id "
				+ "		       left outer join s$column sc "
				+ "		           on sc.column_id  =  l.child_id "
				+ "		       left outer join t$column tc "
				+ "		           on tc.column_id  =  l.child_id "
				+ "		      where sc.column_id is null "
				+ "             and tc.column_id is null " 
				+ "		     group by l.child_id,l.child_db_id "
				+ "		     having count(*) >0 "
				+ "		     order by 1,column_id asc "; 
				
		
	
		String workingInsert = "insert into s$column("
				+ "  column_id"
				+ " , column_db_id"
				+ " , cluster_type"
				+ " , cluster_number"
				+ " , processing_order"
				+ " , pass_number"
				+ " , leading_column_id"
				+ ") select  "
			 	+ "     r.column_id "
			 	+ "     ,r.column_db_id"
			 	+ "     ,'S'"
			 	+ "     ,n.cluster_number"
			 	+ "     ,n.last_processing_order + rownum  as processing_order"
			 	+ "     ,n.pass_number"
			 	+ "     ,r.leading_column_id"
			 	+ "  from (select "
			 	+ "			 cluster_number "
			 	+ "			 ,min(processing_order) as last_processing_order "
			 	+ "          ,cast(? as bigint) as pass_number"
			 	+ "        from s$column "
			 	+ "        where cluster_number = ? "
			 	+ "        group by cluster_number "
			 	+ "    ) n "
			 	+ " cross join t$param p "
			 	+ " cross join ("
			 	+ "  select "
			 	+ "     ri.column_id "
			 	+ "     ,ri.column_db_id"
			 	+ "     ,ri.leading_column_id "
			 	+ "  from ("
			    + "    select "
			    + "       l.child_id as column_id"
			    + "       ,l.child_db_id as column_db_id"
			    + "       ,l.parent_id as leading_column_id"
			    + "    from s$column sc "
			    + "     cross join t$param p "
			    + "     inner join s$link l "
			    + "       on sc.column_id = l.parent_id "
			    + "     inner join s$link lb on lb.parent_id = l.child_id and lb.child_id = l.parent_id"
			    + "     inner join column_info cp on cp.id = l.parent_id"
			    + "     inner join column_info cc on cc.id = l.child_id"
			    + "    where (sc.column_db_id <> l.child_db_id or p.diff_db = 'Y') "
			    + "      and cp.max_sval>=cc.min_sval"
			    + "      and (p.unique_count_sweep is null or "
			 	+ "           l.child_unique_count between (l.parent_unique_count - l.parent_unique_count*p.unique_count_sweep) "
			 	+ "                                and (l.parent_unique_count + l.parent_unique_count*p.unique_count_sweep) ) "
			 	+ "      and (p.total_count_sweep  or "
			 	+ "           l.child_total_count between (l.parent_total_count - l.parent_total_count*p.total_count_sweep) "
			 	+ "                               and (l.parent_total_count + l.parent_total_count*p.total_count_sweep) ) "
			    + "      and l.parent_id = ?"
			    + "   union "
			    + "     select  "
			    + "       l.parent_id as column_id"
			    + "       ,l.parent_db_id as column_db_id"
			    + "       ,l.child_id as leading_column_id"
			    + "    from s$column sc "
			    + "     cross join t$param p "
			    + "     inner join s$link l      "
			    + "       on sc.column_id = l.child_id"
			    + "     inner join s$link lb on lb.parent_id = l.child_id and lb.child_id = l.parent_id"
			    + "     inner join column_info cp on cp.id = l.parent_id"
			    + "     inner join column_info cc on cc.id = l.child_id"
			    + "    where (sc.column_db_id <> l.parent_db_id or p.diff_db = 'Y') "
			    + "      and cc.max_sval>=cp.min_sval"
			    + "      and (p.unique_count_sweep is null or "
			 	+ "           l.parent_unique_count between (l.child_unique_count - l.child_unique_count*p.unique_count_sweep) "
			 	+ "                                 and (l.child_unique_count + l.child_unique_count*p.unique_count_sweep) ) "
			 	+ "      and (p.total_count_sweep is null or "
			 	+ "           l.parent_total_count between (l.child_total_count - l.child_total_count*p.total_count_sweep) "
			 	+ "                                and (l.child_total_count + l.child_total_count*p.total_count_sweep) ) "
			    + "      and l.child_id = ?"
			    + "   ) ri "
			    + "   left outer join s$column sc "
			    + "      on sc.column_id =  ri.column_id "
			    + "   left outer join t$column tc "
			    + "      on tc.column_id = ri.column_id "
			    + "   where tc.column_id is null and sc.column_id is null"
			    + " ) r order by 1"; //for the 			
		  
		String insertSColumn = 
				"insert into s$column(cluster_type,column_id,column_db_id"
				+ ",cluster_number,processing_order,pass_number) "
				+ "  select "
				+ "   'S'"
				+ "   ,cast(? as bigint) as column_id "
				+ "   ,cast(? as bigint) as column_db_id"
				+ "   ,cast(? as bigint) as cluster_number"
				+ "   ,cast(? as bigint) as processing_order"
				+ "   ,cast(? as bigint) as pass_number"
			 	+ "   from t$param p";
			 	
		   
		
		
		
		try (PreparedStatement initialPS = conn.prepareStatement(initialQuery);
				PreparedStatement insertPS = conn.prepareStatement(insertSColumn); 
				PreparedStatement workingPS = conn.prepareStatement(workingInsert)){
			for (;;) {
				Object leadingColumnId = null;
				try (ResultSet rs = initialPS.executeQuery()) {
					if (!rs.next()) {
						break;
					}
					clusterNumber++;
					passNumber = 1;
					leadingColumnId  = rs.getObject("column_id");
					insertPS.setObject(1,leadingColumnId);
					insertPS.setObject(2,rs.getObject("column_db_id"));
					insertPS.setLong(3,clusterNumber);
					insertPS.setLong(4,1);
					insertPS.setLong(5,passNumber);
				}
				insertPS.executeUpdate();
				for (;;) {
					passNumber ++;
					workingPS.setLong(1, passNumber);
					workingPS.setLong(2, clusterNumber);
					workingPS.setObject(3,leadingColumnId );
					workingPS.setObject(4,leadingColumnId );
					if (workingPS.executeUpdate() == 0) {
						break;
					}
					if (diffDb.equals("Y")) {
						break;
					}
				}
			}
		}
		
		
		//merge string clusters and numeric clusters
		execSQL("insert into t$column (column_id,cluster_number,cluster_type,processing_order,pass_number,leading_column_id) "
				+ " select t.column_id,t.cluster_number,cluster_type,t.processing_order,pass_number,leading_column_id from s$column t");
		
		execSQL("delete from link_clustered_column c "
				+ " where (c.workflow_id,c.cluster_label) = ("
				+ "  select p.workflow_id,p.cluster_label from t$param p "
				+ ")");
	
		
	 
		//Reoredering cluster numbers and saving collected columns
		boolean updated = false; 
		try(Statement ps = conn.createStatement()){
			updated = 0 != ps.executeUpdate(	
					"insert into link_clustered_column("
					+ "workflow_id"
					+ ", cluster_label"
					+ ", column_info_id"
					+ ", cluster_number"
					+ ", cluster_type"
					+ ", processing_order"
					+ ", pass_number"
					+ ", leading_column_info_id"
					+ ") "
							+ " direct "
							+ " select p.workflow_id "
							+ "       ,p.cluster_label "
							+ "       ,t.column_id "
							+ "       ,i.renumbered_cluster_number"
							+ "       ,t.cluster_type"
							+ "       ,t.processing_order "
							+ "       ,t.pass_number "
							+ "       ,t.leading_column_id"
							+ "    from t$param p"
							+ "    cross join ("
							+ "       select "
							+ "         rownum as renumbered_cluster_number,"
							+ "         cluster_number from (	"
							+ "            select ti.cluster_number "
							+ "	         	from t$column ti "
							+ "             where ti.cluster_number>0 "
							+ "		        group by ti.cluster_number "
							+ "		        having count(ti.column_id) >=2 " //a cluster must have 2..3 or more columns
							+ "             order by 1 "
							+ "           ) "
							+ "        ) i "
							+ "       inner join t$column t "
							+ "         on t.cluster_number = i.cluster_number "
							+ " ");
		}
		
		if(updated) {
			execSQL("update link_clustered_column_param t set  "
					+ " (t.bitset_level,t.lucene_level) =  "
					+ " (select p.bitset_level, p.lucene_level from t$param p)"
					+ " where (t.workflow_id,t.cluster_label) = "
					+ " (select p.workflow_id,p.cluster_label from t$param p)"
					);
		}
		
		conn.commit();
			
		
	}
	
	





	
	private static final BigDecimal SequenceDeviationThreshold = new BigDecimal(0.03f);
	
	
	/*private static boolean ifNumericType(String realType) {
		
		 return "java.lang.Byte".equals(realType) ||
				"java.lang.Short".equals(realType) ||
				"java.lang.Integer".equals(realType) ||
				"java.lang.Long".equals(realType) ||
				"java.math.BigDecimal".equals(realType);
	}*/
	
	
	
	private static boolean ifSequenceByRange(BigDecimal minValue,BigDecimal maxValue,BigDecimal hashCount) {
		if (hashCount == null) return false;
		if (minValue == null || maxValue == null) return false;
		
		BigDecimal range = maxValue.subtract(minValue).add(BigDecimal.ONE).abs();
		if (range.equals(BigDecimal.ZERO)) {
			return false;
		}
		BigDecimal pct = hashCount.subtract(range).abs().divide(range,BigDecimal.ROUND_HALF_EVEN);
		
		return pct.compareTo(SequenceDeviationThreshold) <= 0;
	}
	
	
	/*private static boolean checkIfColumnSequence(Long column_id ) throws SQLException {
		
		try( PreparedStatement ps = conn.prepareStatement(
						" select real_type,data_scale,max_val,min_val,hash_unique_count "
						+ " from column_info c where c.id = ?") 	) {
			ps.setLong(1, column_id.longValue());
			try (
					ResultSet rs = ps.executeQuery()
					) {
				while (rs.next()) {
					String realType = rs.getString(1);
					Long dataScale = rs.getLong(2);
					String maxSValue = rs.getString(3);
					String minSValue = rs.getString(4);
					BigDecimal hashCount = rs.getBigDecimal(5);
					
					if (!ifNumericType(realType)) return false;
					
					BigDecimal maxValue,minValue;  
					try {
						maxValue = new BigDecimal(maxSValue);
						minValue = new BigDecimal(minSValue);
					} catch (NumberFormatException nfe) {
						return false;
					}
					
					if (maxValue.scale()>0 || minValue.scale()>0) {
						return false;
					}
					
					if (!ifSequenceByRange(minValue, maxValue, hashCount)) return false;
				}
			}
		}
		return true;
	}*/
	
	private static void calculateDumpStats(Long tableId,String astraBasePath) throws SQLException, FileNotFoundException, IOException {
		int maxIntegerValue = Integer.MAX_VALUE; 
		//LocalTime start = LocalTime.now();

		H2Repository repo = new H2Repository();
		TableInfo table = repo.loadTable(tableId);
		System.out.print("Processing ");
		System.out.print(table.schemaName);
		System.out.print(".");
		System.out.print(table.tableName);
		System.out.print("...");
		table.columns = repo.loadTableColumns(tableId);
		astraBasePath = astraBasePath.replaceAll("\"", "");
		char lineDelimiter[] = new char[]{10};
		char columnDelimiter[] = new char[]{31};
		
		int countColumn = table.columns.size();
		try (Scanner lineScanner = new Scanner(new GZIPInputStream(
					new BufferedInputStream(
								new FileInputStream(
										//"C:/home/data.253.4/data/100020/86/ORCL.CRA.LIABILITIES.dat"
										astraBasePath+table.pathToFile
										),10*1024
								)
					)
				)) {
			//s.useDelimiter(new String(delimiter));
			lineScanner.useDelimiter(String.valueOf(lineDelimiter));
			while (lineScanner.hasNext()) {
				String line = lineScanner.next();
				int columnIndex = -1;
				try (Scanner columnScanner = new Scanner(line)) {
					columnScanner.useDelimiter(String.valueOf(columnDelimiter));
					while (columnScanner.hasNext()) {
						columnIndex ++;
						String stringColumnData = columnScanner.next();
						if (stringColumnData == null || stringColumnData.isEmpty()) continue;
						ColumnInfo columnInfo = table.columns.get(columnIndex );
						if (columnInfo.hasNumericContent == null) columnInfo.hasNumericContent = Boolean.FALSE;
						
						
						if (columnInfo.minSValue == null) {
							columnInfo.minSValue = stringColumnData;
							columnInfo.maxSValue = stringColumnData;
						} else {
							if (columnInfo.minSValue.compareTo(stringColumnData) > 0) {
								columnInfo.minSValue = stringColumnData;
							}
							if (columnInfo.maxSValue.compareTo(stringColumnData) < 0) {
								columnInfo.maxSValue = stringColumnData;
							}
						}
						double numericColumnData;
						try{
							numericColumnData = Double.parseDouble(stringColumnData);
							columnInfo.hasNumericContent=Boolean.TRUE;
						} catch (NumberFormatException e){
							continue;
						}
						if (columnInfo.minFValue == null) {
							columnInfo.minFValue = new Double(0);
							columnInfo.maxFValue = new Double(0);
							columnInfo.auxMinFValue = numericColumnData;
							columnInfo.auxMaxFValue = numericColumnData;
						} else {
							columnInfo.auxMinFValue = Double.min(columnInfo.auxMinFValue , numericColumnData);
							columnInfo.auxMaxFValue = Double.max(columnInfo.auxMaxFValue , numericColumnData);
						}
						boolean positive = numericColumnData>=0;
						
						numericColumnData = Math.abs(numericColumnData);
						
						if (numericColumnData - Math.ceil(numericColumnData) == 0 ) {

							long longImage = Math.round(numericColumnData);
							Long key = new Long(longImage >> (4 * 8 - 1)); //4bytes minus 1bit for the sing
							int value = (int) longImage & 0x7FFFFFFF; 
							if (maxIntegerValue == value) continue;

							SparseBitSet bs = null;
							if (columnInfo.positiveBitsets == null) {
								columnInfo.positiveBitsets = new TreeMap<>();
								columnInfo.negativeBitsets = new TreeMap<>();
							}
							if (positive) 
								bs = columnInfo.positiveBitsets.get(key);
							else
								bs = columnInfo.negativeBitsets.get(key);
							
							if (bs == null) {
								bs = new SparseBitSet();
								if (positive) 
									columnInfo.positiveBitsets.put(key,bs);
								else
									columnInfo.negativeBitsets.put(key,bs);
							}
							bs.set(value);
						} else {
							columnInfo.hasFloatContent = Boolean.TRUE;
						}
					}
					if (countColumn < columnIndex+1) {
						throw new RuntimeException(String.format("Column number mismatch (%d<->%d) for line #d : %s",countColumn,(columnIndex+1),line));
					}
				};
				
			};
		}
			
		class StatPiece{
			boolean negative;
			Long key;
			double cardinality,	movingMean, standardDeviation;
			public void process(SparseBitSet bs) {
				int prevValue = 0, value = 0;
				long cumulativeDeviaton = 0;
				this.cardinality  = (double)bs.cardinality();
				boolean gotPrevValue = false;
				gotPrevValue = bs.get(0); // It's not possible to check 0 value in the cycle below
				for(;;) {
					value = bs.nextSetBit(value + 1);
					if (value == -1) break;
					if (!gotPrevValue) {
						prevValue = value;
						gotPrevValue = true;
					} else {
						cumulativeDeviaton += (value - prevValue);
						prevValue = value;
					}
				}
				this.movingMean = cumulativeDeviaton/(this.cardinality-1);
				prevValue= 0;value = 0;
				double totalDeviation = 0;  
				gotPrevValue = bs.get(0);
				for(;;) {
					value = bs.nextSetBit(value + 1);
					if (value == -1) break;
					if (!gotPrevValue) {
						prevValue = value;
						gotPrevValue = true;
					} else {
						totalDeviation = totalDeviation + Math.pow(this.movingMean - (double)(value - prevValue),2);
						prevValue = value;
					}
				}
				this.standardDeviation = Math.sqrt(totalDeviation/(this.cardinality-1));
			}
						
		}
		List<StatPiece> stats = new ArrayList<>();
	
		
		for(ColumnInfo column : table.columns) {
			if (column.hasNumericContent != null && column.hasNumericContent) {
				column.maxFValue = new Double(column.auxMaxFValue);
				column.minFValue = new Double(column.auxMinFValue);
				
				if (column.hasFloatContent == null) column.hasFloatContent = Boolean.FALSE;
				
				if (column.positiveBitsets != null && column.positiveBitsets.size()>0) {
					Set<Long> keySet = column.positiveBitsets.keySet();
					List <Long>keys = Collections.list(Collections.enumeration(keySet));
					Collections.sort(keys);
					for (Long key:keys) {
						SparseBitSet bs = column.positiveBitsets.get(key);
						if (bs.cardinality()>1) {
							StatPiece ps = new StatPiece();
							ps.key = key;
							ps.negative = false;
							ps.process(bs);
							stats.add(ps);
						}
					}
				}
				if (column.negativeBitsets != null && column.negativeBitsets.size()>0) {
					Set<Long> keySet = column.negativeBitsets.keySet();
					List <Long>keys = Collections.list(Collections.enumeration(keySet));
					Collections.sort(keys);
					for (Long key:keys) {
						SparseBitSet bs = column.negativeBitsets.get(key);
						if (bs.cardinality()>1) {
							StatPiece ps = new StatPiece();
							ps.key = key;
							ps.negative = true;
							ps.process(bs);
							stats.add(ps);
						}
					}
				}
				column.negativeBitsets = null;
				column.positiveBitsets = null;
				
				if (stats.size()>0) {
					
					StatPiece total;
					if (stats.size() == 1)
						total = stats.get(0);
					else {
						total = new StatPiece();
						for (StatPiece ps: stats) {
							total.movingMean += ps.movingMean;
							total.standardDeviation += ps.standardDeviation*ps.cardinality;
							total.cardinality +=ps.cardinality; 
						}
						total.movingMean = total.movingMean/(double)stats.size();
						total.standardDeviation = total.standardDeviation/total.cardinality;
					}
					column.integerUniqueCount = new Long(Math.round(total.cardinality));
					column.movingMean = new Double(total.movingMean);
					column.standardDeviation = new Double(total.standardDeviation);
				}
			}
			repo.saveColumnStats(column);
			
		}
		System.out.println(" Done");
	}
	
	/*
	
	
	private static void calculateColStats(ColumnStats stats,List<String> params,Properties parsedArgs) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		Connection targetConnection = null;
		String url = null, targetQuery = null;
		BigDecimal dataScale = null;
		BigDecimal minValue = null,maxValue = null;
		boolean movingMeanCalc = params.contains("MOVING_MEAN");
		boolean BucketsCalc = params.contains("BUCKETS");
		try (
		PreparedStatement ps = conn.prepareStatement(
				"select "
				+ "  conf.target"
				+ ", conf.username"
				+ ", conf.password"
				+ ", conf.host"
				+ ", conf.port"
				+ ", conf.database_name"
				+ ", col.name as column_name"
				+ ", tab.schema_name"
				+ ", tab.name as table_name"
				+ ", col.data_scale"
				+ ", st.num_max_val"
				+ ", st.num_min_val"
				+ " from column_info col "
				+ "  inner join table_info tab  on tab.id = col.table_info_id "
				+ "  inner join metadata mtd on mtd.id = tab.metadata_id "
				+ "  inner join database_config conf on conf.id = mtd.database_config_id "
				+ "  inner join column_numeric_stats st on st.column_id = col.id "
				+ " where col.id = ?")
				) {
			ps.setBigDecimal(1, stats.columnId);
			try (ResultSet rs = ps.executeQuery()){
				while (rs.next()) {
					String className = null;
					String uid = rs.getString("username"),
						   pwd = rs.getString("password"); 
					if ("ORACLE".equals(rs.getString("target"))) {
						url = String.format("jdbc:oracle:thin:@%s:%d:%s", rs.getString("host"),rs.getInt("port"),rs.getString("database_name"));
						className = "oracle.jdbc.OracleDriver";
					} else	if ("SYBASE".equals(rs.getString("target"))) {
						url = String.format("jdbc:jtds:sybase://%s:%d/%s", rs.getString("host"),rs.getInt("port"),rs.getString("database_name"));
						className = "net.sourceforge.jtds.jdbc.Driver";
					} else	if ("MSSQL".equals(rs.getString("target"))) {
						url = String.format("jdbc:jtds:sqlserver://%s:%d/%s", rs.getString("host"),rs.getInt("port"),rs.getString("database_name"));
						className = "net.sourceforge.jtds.jdbc.Driver";
					}
					dataScale = rs.getBigDecimal("data_scale");
					Driver driver = (Driver) Class.forName(className).newInstance();
					Properties p = new Properties();
					p.put("user", uid);
					p.put("password", pwd);
					targetConnection = driver.connect(url, p);
					targetQuery = String.format("select %s from %s.%s where %1$s is not null",rs.getString("column_name"),rs.getString("schema_name"),rs.getString("table_name"));
					minValue = rs.getBigDecimal("num_min_val");
					maxValue = rs.getBigDecimal("num_max_val");
					
					break;
				}
			}
		}
		if (targetConnection == null) 
				throw new RuntimeException(String.format("No connection created for column_info_id = %d; url =%s\n",stats.columnId, url));
		SparseBitSet sbp = new SparseBitSet();
		SparseBitSet sbn = new SparseBitSet();
		long bucketDivisor = 0;
		long shift = 0;
		Map<BigDecimal,BigInteger> buckets = null; 
		
		if (BucketsCalc) {
			String sval = parsedArgs.getProperty("bucket");
			if (sval == null || sval.isEmpty()) {
				throw new RuntimeException("--bucket parameter is expected");
			}
			buckets = new TreeMap<>();
			bucketDivisor = (new BigDecimal(sval)).longValue();
			shift = minValue.longValue()%bucketDivisor;  
		}
		try(PreparedStatement ps = targetConnection.prepareStatement(targetQuery); 
				ResultSet rs = ps.executeQuery()){
			    ps.setFetchSize(50000);
			while (rs.next()) {
				BigDecimal columnValue = rs.getBigDecimal(1);
				
				if (columnValue == null ) continue;
				
				if (movingMeanCalc && columnValue.scale()>0) {
					movingMeanCalc = false;
				}
				if (movingMeanCalc) {
					if (columnValue.signum() == -1) 
						sbn.set(-1*columnValue.intValueExact());
					 else 
						sbp.set(columnValue.intValueExact());
				}
				if (BucketsCalc) {
					float fkey = (columnValue.longValue() - shift)/bucketDivisor;
					
					BigDecimal mapKey = new BigDecimal(fkey);
					BigInteger countValue = buckets.get(mapKey);//map.get(mapKey);
					if (countValue == null) {
						countValue = BigInteger.ONE;
					} else {
						countValue = countValue.add(BigInteger.ONE);
					}
					buckets.put(mapKey, countValue);
					//map.put(mapKey, countValue);
				}
			}
		} catch(SQLException e) {
			throw new RuntimeException("Error while executing query "+targetQuery,e); 
		}
		targetConnection.close();

		if (movingMeanCalc) {
			long collectiveStep = 0;
			int curr = 0, prev = -1;
			if (sbp.size() > 1) {
				int halfSize = (int)(sbp.cardinality()/2d);
				int counter = 1;
				while(true) {
					curr = sbp.nextSetBit(prev+1);
					if (curr == -1) break;
					counter++;
					if (prev != -1) {
						collectiveStep += curr - prev;  
	
						if ( counter > halfSize && stats.median == null) {
							stats.median = new BigDecimal(prev);
						}
						
					}
					prev = curr;
				}
				double movingMean = collectiveStep / (double)(sbp.cardinality()-1);
				
				double collectiveSqrs = 0d; 
				curr = 0;
				prev = -1;
				while(true) {
					curr = sbp.nextSetBit(prev+1);
					if (curr == -1) break;
					if (prev != -1) {
						int delta = (curr - prev);
						collectiveSqrs = collectiveSqrs + Math.pow(movingMean  - (double)delta,2d);
					}
					 prev = curr;
				}
				stats.movingMean  = new BigDecimal(movingMean);
				stats.stdDev  = new BigDecimal(Math.sqrt(collectiveSqrs/(double)(sbp.cardinality())));
			}
		}
		if(BucketsCalc) {
			System.out.println(targetQuery);
				for (Map.Entry<BigDecimal,BigInteger> entry: buckets.entrySet()){
					try(
							//DB bucketDB = DBMaker.fileDB(String.format("./c%d.mapdb",stats.columnId)).fileMmapEnable().readOnly().make();
							//BTreeMap<BigDecimal,BigInteger> map = bucketDB.treeMap("c",Serializer.BIG_DECIMAL,Serializer.BIG_INTEGER).open();
							PreparedStatement ps = conn.prepareStatement("merge into column_numeric_bucket "
									+ "(column_id,bucket_width,bucket_no,hits) "
									+ " key (column_id,bucket_width,bucket_no) "
									+ "values(?,?,?,?)") 
							) {
					ps.setBigDecimal(1, stats.columnId);
					ps.setLong(2, bucketDivisor);
					ps.setBigDecimal(3, entry.getKey());
					ps.setLong(4, entry.getValue().longValue());
					
					System.out.print(entry.getKey());
					System.out.print(" - ");
					System.out.println(entry.getValue());

					ps.execute();
					conn.commit();
					}
				}

		}
	}
	*/
	private static void makeTableColStats() throws SQLException {
		execSQL("create table if not exists column_numeric_bucket( "
				+ " column_id bigint "
				+ "  ,bucket_width bigint "
				+ "  ,bucket_no bigint "
				+ "  ,hits bigint "
				+ "  ,constraint column_numeric_bucket_pk primary key (column_id,bucket_width,bucket_no) "
				+ " )");
	}
	
	private static void makeTableNumericRealType() throws SQLException {
		
				 
		execSQL(
				" create table if not exists column_numeric_real_type( "
				+ "	  real_type varchar(255), "
				+ "	  constraint column_numeric_real_type_pk primary key (real_type) "
				+ "	); "
				+ " merge into column_numeric_real_type (real_type) key(real_type) values ('java.lang.Byte'); "
				+ " merge into column_numeric_real_type (real_type) key(real_type) values ('java.lang.Short'); "
				+ " merge into column_numeric_real_type (real_type) key(real_type) values ('java.lang.Integer'); "
				+ " merge into column_numeric_real_type (real_type) key(real_type) values ('java.lang.Long'); "
				+ " merge into column_numeric_real_type (real_type) key(real_type) values ('java.math.BigDecimal'); "
				+ " alter table column_info add column if not exists has_numeric_content boolean; "
				+ " alter table column_info add column if not exists has_float_content boolean; "
				+ " alter table column_info add column if not exists min_fval double;"
				+ " alter table column_info add column if not exists max_fval double;"
				+ " alter table column_info add column if not exists min_sval varchar(4000);"
				+ " alter table column_info add column if not exists max_sval varchar(4000);"
				+ " alter table column_info add column if not exists integer_unique_count bigint;"
				+ " alter table column_info add column if not exists moving_mean double;"
				+ " alter table column_info add column if not exists moving_stddev double;"
				+ " alter table column_info add column if not exists position_in_pk int;"
				+ " alter table column_info add column if not exists total_in_pk int;"
				);
		
		execSQL("drop view if exists public.column_info_numeric_range_view; "
				+ "create or replace view public.column_info_numeric_range_view as  "
		        +" select "
		        +"   c.id "
		        + "  ,c.name "
		        + "  ,c.table_info_id "
		        +"   ,c.has_numeric_content "
		        + "  ,c.has_float_content "
		        +"   ,c.min_fval as min_val "
		        +"   ,c.max_fval as max_val "
		        + "  ,nvl(c.unique_row_count,c.hash_unique_count) unique_count"
		        + "  ,c.total_row_count as total_count"
		        +" from public.column_info c "
		        +" where c.min_fval is not null "
		        +"   and c.max_fval is not null "
		        + "  and c.has_numeric_content = true ");
	}
		        

	
	
	/*private static ColumnStats getColStats(BigDecimal columnId) throws SQLException {
		ColumnStats result  = null;
		try(PreparedStatement ps = conn.prepareStatement(
				"select moving_mean"
				+ ", std_dev"
				+ ", median"
				+ ", is_sequence"
				+ ", num_min_val"
				+ ", num_max_val "
				+ ", position_in_pk"
				+ ", total_in_pk"
				+ " from column_numeric_stats where column_id = ? ")) {
			ps.setLong(1, columnId.longValue());
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					result = new ColumnStats();
					result.columnId = columnId;
					result.movingMean = rs.getBigDecimal("moving_mean");
					result.stdDev = rs.getBigDecimal("std_dev");
					result.median = rs.getBigDecimal("median");
					result.isSequence = rs.getBoolean("is_sequence");
					result.numMin = rs.getBigDecimal("num_min_val");
					result.numMax = rs.getBigDecimal("num_max_val");
					result.positionInPk = rs.getBigDecimal("position_in_pk");
					result.totalInPk = rs.getBigDecimal("total_in_pk");
					
				}
			}
		}
		return result;
	}
	
	
	private static void saveColStats(ColumnStats stats ) throws SQLException {
		try(PreparedStatement ps = conn.prepareStatement(
				"merge into column_numeric_stats("
				+ " column_id"
				+ " ,moving_mean"
				+ " ,std_dev"
				+ " ,median"
				+ " ,is_sequence"
				+ " ,num_min_val"
				+ " ,num_max_val"
				+ " ,position_in_pk"
				+ " ,total_in_pk"
				+ ") key(column_id) "
				+ "values (?,?,?,?,?,?,?,?,?)")) {
			ps.setBigDecimal(1,stats.columnId);
			ps.setBigDecimal(2,stats.movingMean);
			ps.setBigDecimal(3,stats.stdDev);
			ps.setBigDecimal(4,stats.median);
			ps.setBoolean(5,stats.isSequence);
			ps.setBigDecimal(6,stats.numMin);
			ps.setBigDecimal(7,stats.numMax);
			ps.setBigDecimal(8,stats.positionInPk);
			ps.setBigDecimal(9,stats.totalInPk);
			ps.execute();
		}
		execSQL("Commit");
	}
	
	
	
	private static void pairStatistics(Long workflow_id, List<String> params, Properties parsedArgs) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		boolean found = false;
		try(
				PreparedStatement ps = conn.prepareStatement(
						" select r.id"
						+ "    ,c.max_fval"
						+ "    ,c.min_fval"
						+ "    ,c.integer_unique_count"
						+ "    ,nvl(c.data_scale,0) as data_scale "
						+ "    ,cnc.position_in_constraint "
						+ "    ,case when cn.id is not null then ("
						+ "           select count(*) from constraint_column_info c1 "
						+ "           where c1.constraint_info_id = cn.id) end as total_in_pk"
						+ "  from ("
						+ "   select parent_column_info_id as id from link l  where l.WORKFLOW_ID = ? "
						+ "    union "
						+ "   select child_column_info_id as id from link l  where l.WORKFLOW_ID = ? "
						+ " ) r inner join column_info c on c.id = r.id "
						+ "     left outer join constraint_column_info cnc on cnc.child_column_id = r.id"
						+ "     left outer join constraint_info cn on cn.id = cnc.constraint_info_id and cn.constraint_type='PK'"
						+ " where c.has_numeric_content = true "
					)
				) {
			ps.setLong(1, workflow_id);
			ps.setLong(2, workflow_id);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					found = true;
					BigDecimal columnId = rs.getBigDecimal("id");
					ColumnStats stats  = getColStats(columnId);
					if (stats == null) {
						stats = new ColumnStats();
						stats.columnId = columnId;
					}
					stats.isSequence = new Boolean(rs.getInt("data_scale")==0);
					
					stats.positionInPk = rs.getBigDecimal("position_in_constraint");
					stats.totalInPk = rs.getBigDecimal("total_in_pk");
					
					
					
					if (params.contains("IS_SEQ") && stats.isSequence ) {
							try {
								stats.numMin = new BigDecimal(rs.getString("min_val"));
								stats.numMax = new BigDecimal(rs.getString("max_val"));
								stats.isSequence = stats.numMax.scale() == 0 && stats.numMax.scale() == 0; 
							} catch (NumberFormatException nfe) {
								stats.isSequence = false;
								stats.numMin = null;
								stats.numMax = null;
							}
						stats.isSequence = stats.isSequence ? ifSequenceByRange(stats.numMin, stats.numMax, rs.getBigDecimal("hash_unique_count"))
								      : stats.isSequence ;   
							
						saveColStats(stats);
					}
					calculateColStats(stats,params,parsedArgs);
					saveColStats(stats);
				}
			}
		}
		if (!found) {
			throw new RuntimeException("No pairs found to process");
		}
	}*/
	
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
						out.write(".integer {mso-number-format:\"0\";text-align:right;}");
						out.write(".centered {text-align:center;}");
						out.write("</STYLE>");
						out.write("</HEADER>");
						out.write("<BODY>");
						out.write("<P style='font-weight:bold;'>");
						out.write("Workflow ID: "); out.text(String.valueOf(workflowId));
						out.write("; Label: ");		out.text(clusterLabel);
						out.write("; Bitset Confidence Level: ");	out.textf("%f",rs.getBigDecimal("BITSET_LEVEL"));
						out.write("; Lucene Confidence Level: "); 	out.textf("%f",rs.getBigDecimal("LUCENE_LEVEL"));
						out.write(";</P>");
						out.write("<TABLE BORDER>");
						out.write("<col width=50>"
						 +"<col width=100>"
						 +"<col width=128>"
						 +"<col width=150>"
						 +"<col width=175>"
						 +"<col width=100>"
						 +"<col width=120>"
						 +"<col width=150>"
						 +"<col width=175>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=200>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=100>"
						 +"<col width=200>"
						 + "");
						
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
						
						out.element("TH", "Parent Sequential Integers");
						out.element("TH", "Parent distinct count");
						out.element("TH", "Parent min");
						out.element("TH", "Parent max");
						out.element("TH", "Parent mapped type");

						out.element("TH", "Child Sequential Integers");
						out.element("TH", "Child distinct count");
						out.element("TH", "Child min");
						out.element("TH", "Child max");
						out.element("TH", "Child mapped type");
							
						out.write("</TR>");
					}
					out.write("<TR>");

					out.elementf("TD", "class='integer'", "%d", rs.getLong("CLUSTER_NUMBER"));
					out.element("TD", rs.getString("PARENT_DB_NAME"));
					out.element("TD", rs.getString("PARENT_SCHEMA_NAME"));
					out.element("TD", rs.getString("PARENT_TABLE_NAME"));
					out.element("TD", rs.getString("PARENT_COLUMN_NAME"));

					out.element("TD", rs.getString("CHILD_DB_NAME"));
					out.element("TD", rs.getString("CHILD_SCHEMA_NAME"));
					out.element("TD", rs.getString("CHILD_TABLE_NAME"));
					out.element("TD", rs.getString("CHILD_COLUMN_NAME"));
					// bit_set_exact_similarity
					out.elementf("TD","class='confidence'", "%f", rs.getBigDecimal("BS_CONFIDENCE"));

					// lucine_sample_term_similarity
					out.elementf("TD","class='confidence'", "%f"	, rs.getBigDecimal("LC_CONFIDENCE"));

					// rev_bit_set_exact_similarity
					out.elementf("TD","class='confidence'", "%f", rs.getBigDecimal("REV_BS_CONFIDENCE"));

					// rev_lucine_sample_term_similarity
					out.elementf("TD","class='confidence'", "%f", rs.getBigDecimal("REV_LC_CONFIDENCE"));
					
					out.elementf("TD","class='centered'", "%s", rs.getString("parent_is_sequence")); 
					out.elementf("TD","class='integer'", "%d", rs.getObject("PARENT_HUQ"));
					out.elementf("TD","class='confidence'", "%f", rs.getBigDecimal("PARENT_MIN"));
					out.elementf("TD","class='confidence'", "%f", rs.getBigDecimal("PARENT_MAX"));
					out.elementf("TD","%s", rs.getString("PARENT_REAL_TYPE"));

					out.elementf("TD","class='centered'", "%s", rs.getString("child_is_sequence")); 
					out.elementf("TD","class='integer'", "%d", rs.getObject("CHILD_HUQ"));
					out.elementf("TD","class='confidence'", "%f", rs.getBigDecimal("CHILD_MIN"));
					out.elementf("TD","class='confidence'", "%f", rs.getBigDecimal("CHILD_MAX"));
					out.elementf("TD","%s", rs.getString("CHILD_REAL_TYPE"));
					
					
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
		throws SQLException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
			Locale.setDefault(Locale.US);

			if (workflowId == null) {
				throw new RuntimeException("Error: Workflow ID has not been specified!");
			}

			if (outFile == null || outFile.isEmpty()) {
				throw new RuntimeException("Error: Output file has not been specified!");
			}
			
			
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
				Set<Long> bucketColumnIds = new TreeSet<>(); 
				try (ResultSet rs = ps.executeQuery();
						HTMLFileWriter out = new HTMLFileWriter(outFile)) {
					while(rs.next()) {
						counter++;
						if (counter == 1) {
							out.write("<HTML>\n");
							out.write("<HEADER>\n");
							out.write("<meta http-equiv=Content-Type content='text/html; charset=UTF-8'>\n");
							out.write("<STYLE>\n");
							out.write(".confidence {\n"
									+ " mso-number-format:\"0\\.00000\";\n"
									+ " text-align:right;\n"
									+ "}\n"
									+ ".integer {\n"
									+ "  mso-number-format:\"0\";\n"
									+ "  text-align:right;\n"
									+ "}\n"
									+ ".centered {\n"
									+ "  text-align:center;\n"
									+ "}\n"
									+ ".modal { \n"
									+ "display: none; /* Hidden by default */\n"
									+ "position: fixed; /* Stay in place */\n"
								    + "z-index: 1; /* Sit on top */\n"
								    + "left: 0;\n"
								    + "top: 0;\n"
								    + "width: 100%; /* Full width */\n"
								    + "min-width:1200;\n"
								    + "height: 100%; /* Full height */\n"
								    + "overflow: auto; /* Enable scroll if needed */\n"
								    + "background-color: rgb(0,0,0); /* Fallback color */\n"
								    + "background-color: rgba(0,0,0,0.4); /* Black w/ opacity */\n"
								    + "margin: 0; /* % from the top and centered */\n"
								    + "padding: 0;\n"
								    + "}\n"
								    + ".modal-content {\n"
								    + " background-color: #fefefe;\n"
								    + "margin: 5% auto; /* % from the top and centered */\n"
								    + "padding: 20px;\n"
								    + "border: 1px solid #888;\n"
								    + "width: 80%; /* Could be more or less, depending on screen size */\n"
								    + "}\n");
							out.write("</STYLE>\n");
							out.write("<script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>\n");
							out.write("<script type=\"text/javascript\">\n"
									+ " google.charts.load('current', {'packages':['corechart']});\n"
									+ " function charts(dataSource1,dataSource2,norm){"
									+ "  let pairs = []; \n"
									+ "  if (dataSource1 != null) 	pairs.push(dataSource1()); \n"
									+ "  if (dataSource2 != null) 	pairs.push(dataSource2()); \n"
									+ "  let prep=new Map();\n"
									+ "  let data=new google.visualization.DataTable();\n"
									+ "  data.addColumn('number','Column data');\n"
									+ "  for(let e in pairs) {"
									+ "    data.addColumn('number', pairs[e].entry_name);\n"
									+ "    for (let b of pairs[e].buckets){\n"
									+ "      let key = b.bucket_no*pairs[e].bucket_width;\n"
									+ "      var bothHits = prep.get(key); \n"
									+ "      if (bothHits == null) \n"
									+ "		 	  bothHits = {}; \n"
									+ "      if (norm)\n"
									+ "           bothHits['hit_'+e] = 1;\n "
									+ "      else \n"
									+ "           bothHits['hit_'+e] = b.hits;\n "
									+ "      prep.set(key,bothHits);\n "
									+ "    }\n"
									+ "  }\n"
									+ "  let rows = [];\n"
									+ "  for( var [k,v] of prep) {\n"
									+ "     let row=[]; \n"
									+ "     row.push(k); \n"
									+ "     row.push(v['hit_0']); \n"
									+ "     if (pairs.length === 2) row.push(v['hit_1']); \n"
									+ "     rows.push(row); \n"
									+ "  } \n"
									+ "  data.addRows(rows);\n"
									+ "  var options = {\n"
									+ "   title: 'Column '+(pairs.length === 2 ? 'pair ' : '')+' data distribution',\n"
						       		+ "   /*curveType: 'function',\n*/"
						       		+ "   is3D: true,\n"
						       		+ "   legend: { position: 'bottom' },\n"
						            + "   width: 1200,\n"
						            + "   height: 800,\n"
						            + "   hAxis: { title: 'Data values'},\n"
						            + "   vAxis: { title: 'Hits'},\n"
						            + "   colors: ['#a52714', '#097138']\n"
						            + "  };\n"
						            + "  modal.style.display='block';"
						            + "  let chart = new google.visualization.LineChart(document.getElementById('chart'));\n"
						            + "  chart.draw(data, options);\n"
						            + "}\n");
							out.write("</script>\n");
							out.write("</HEADER>\n");
							out.write("<BODY>\n");
							out.write("<P style='font-weight:bold;'> Workflow ID: ");
							out.write(String.valueOf(workflowId));
							out.write("</P>\n");
							out.write("<TABLE BORDER>\n");
							out.write(""+
							//Parent db entry
							"<col width=100>"+
							 "<col width=128>"+
							 "<col width=150>"+
							 "<col width=175>"+

							 //Child db entry
							 "<col width=100>"+
							 "<col width=120>"+
							 "<col width=150>"+
							 "<col width=175>"+
							
							 //Confidence columns
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 
							 //indicators
							 "<col width=75>"+
							 "<col width=75>"+

							 //Parent stats
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							
							 //child stats
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 "<col width=100>"+
							 // Ids
							 "<col width=50>"+
							 "<col width=50>"+
							 "<col width=50>"+
							 "<col width=50>"+
							 "\n");
							
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
							out.element("TH", "Equal distinct count");
							
							out.element("TH", "Parent Sequential Integers");
							out.element("TH", "Parent distinct count");
							out.element("TH", "Parent min value");
							out.element("TH", "Parent max value");
							out.element("TH", "Parent median");
							out.element("TH", "Parent moving mean");
							out.element("TH", "Parent standard deviation of moving mean");
							out.element("TH", "Parent mapped type");
							

							out.element("TH", "Child Sequential Integers");
							out.element("TH", "Child distinct count");
							out.element("TH", "Child min value");
							out.element("TH", "Child max value");
							out.element("TH", "Child median");
							out.element("TH", "Child moving mean");
							out.element("TH", "Child standard deviation of moving mean");
							out.element("TH", "Child mapped type");

							out.element("TH", "Sequence range similarity");

							out.element("TH", "Data bucket charts");

							out.element("TH", "Parent numeric data scale");
							out.element("TH", "Child numeric data scale");
							out.element("TH", "Numeric Data scale difference");

							out.element("TH", "Parent position in PK");
							out.element("TH", "Parent total columns in PK");
							out.element("TH", "Child position in PK");
							out.element("TH", "Child total columns in PK");
							out.element("TH", "Rule#7: PK-PK");
							
							out.element("TH", "Rule#8: Against parent PK");
							out.element("TH", "Rule#8: Against child PK");							                                                                                                                               
							out.element("TH", "Link ID");
							out.element("TH", "Reversal Link ID");

							out.write("</TR>\n");
						}
						
						out.write("<TR>");

						out.element("TD", rs.getString("PARENT_DB_NAME"));
						out.element("TD", rs.getString("PARENT_SCHEMA_NAME"));
						out.element("TD", rs.getString("PARENT_TABLE_NAME"));
						out.element("TD", rs.getString("PARENT_COLUMN_NAME"));

						out.element("TD", rs.getString("CHILD_DB_NAME"));
						out.element("TD", rs.getString("CHILD_SCHEMA_NAME"));
						out.element("TD", rs.getString("CHILD_TABLE_NAME"));
						out.element("TD", rs.getString("CHILD_COLUMN_NAME"));

						// bit_set_exact_similarity
						out.elementf("TD","class='confidence'", "%f", rs.getObject("bs_confidence"));

						// lucine_sample_term_similarity
						out.elementf("TD","class='confidence'", "%f"	, rs.getObject("lc_confidence"));

						// rev_bit_set_exact_similarity
						out.elementf("TD","class='confidence'", "%f", rs.getObject("rev_bs_confidence"));

						// rev_lucine_sample_term_similarity
						out.elementf("TD","class='confidence'", "%f", rs.getObject("rev_lc_confidence"));


						// Bitset group
						out.elementf("TD","class='integer'", "%d", rs.getObject("bitset_group_num"));

						// Distinct count
						out.element("TD", "class='centered'",rs.getString("unique_same"));

						out.element("TD", "class='centered'", rs.getString("parent_is_sequence"));
						out.elementf("TD","class='integer'", "%d", rs.getObject("parent_huq"));
						out.elementf("TD","class='integer'", "%f", rs.getBigDecimal("parent_min"));
						out.elementf("TD","class='integer'", "%f", rs.getBigDecimal("parent_max"));
						out.elementf("TD", "class='integer'", "%f",rs.getBigDecimal("parent_median"));
						out.elementf("TD", "class='integer'", "%f",rs.getBigDecimal("parent_moving_mean"));
						out.elementf("TD", "class='confidence'", "%f",rs.getBigDecimal("parent_std_dev"));
						out.element("TD", rs.getString("parent_real_type"));
						

						out.element("TD", "class='centered'", rs.getString("child_is_sequence"));
						out.elementf("TD","class='integer'", "%d", rs.getObject("child_huq"));
						out.elementf("TD","class='integer'", "%f", rs.getBigDecimal("child_min"));
						out.elementf("TD","class='integer'", "%f", rs.getBigDecimal("child_max"));
						out.elementf("TD", "class='integer'", "%f",rs.getBigDecimal("child_median"));
						out.elementf("TD", "class='confidence'", "%f",rs.getBigDecimal("child_moving_mean"));
						out.elementf("TD", "class='confidence'", "%f",rs.getBigDecimal("child_std_dev"));
						out.element("TD", rs.getString("child_real_type"));
						
						out.elementf("TD", "class='confidence'", "%f",rs.getBigDecimal("range_similarity"));
						
						//BT
						out.write("<TD class='centered' nowrap>");
						{ String parent_buckets = rs.getString("parent_buckets");
						  String child_buckets = rs.getString("child_buckets");
							if ( parent_buckets != null || child_buckets != null ) {
								/*out.write("<input type='button' "
										+ "value='Data bucket" 
										+	((parent_buckets != null && child_buckets != null)? "s' " : "' ")
										+ "onClick='charts("
										+ Objects.toString(parent_buckets, "null")
										+ "," 
										+ Objects.toString(child_buckets, "null")
										+")'/>");*/
								out.write("<input type='button' "
								+ "value='Hits" 
								+	((parent_buckets != null && child_buckets != null)? "(2)' " : "(1)' ")
								+ "onClick='charts("
								+ Objects.toString(parent_buckets, "null")
								+ "," 
								+ Objects.toString(child_buckets, "null")
								+ ", false" 
								+")'/>");
								out.write("<input type='button' "
										+ "value='Norm" 
										+	((parent_buckets != null && child_buckets != null)?"(2)' " : "(1)'")
										+ "onClick='charts("
										+ Objects.toString(parent_buckets, "null")
										+ "," 
										+ Objects.toString(child_buckets, "null")
										+ ", true" 
										+")'/>");
							}
							if (parent_buckets != null) { 
								bucketColumnIds.add(rs.getLong("parent_column_info_id"));
							}
							if (child_buckets != null) { 
								bucketColumnIds.add(rs.getLong("child_column_info_id"));
							}
						}
						out.write("</TD>");
						
						{
							BigDecimal parentDataScale = rs.getBigDecimal("parent_data_scale"),
								childDataScale = rs.getBigDecimal("child_data_scale");
						
							out.elementf("TD","class='integer'", "%d",
									(parentDataScale!=null ? parentDataScale.intValue() : null));
							out.elementf("TD","class='integer'", "%d", 
									(childDataScale!=null ? childDataScale.intValue() : null));
							out.elementf("TD","class='centered'", "%s", 
									(parentDataScale != null && childDataScale != null &&
											parentDataScale.intValue() != childDataScale.intValue()? "Y" :null));
						}
						
						{BigDecimal parentColumnPosInPk = rs.getBigDecimal("parent_position_in_constraint"), 
								parentTotalColumnsInPk = rs.getBigDecimal("parent_total_columns_in_pk"),
								childColumnPosInPk = rs.getBigDecimal("child_position_in_constraint"),
								childTotalColumnsInPk = rs.getBigDecimal("child_total_columns_in_pk");
							
							out.elementf("TD","class='integer'", "%d",
									(parentColumnPosInPk!=null?parentColumnPosInPk.intValue():null));
							out.elementf("TD","class='integer'", "%d", 
									(parentTotalColumnsInPk!=null?parentTotalColumnsInPk.intValue():null));
							
							out.elementf("TD","class='integer'", "%d",
									(childColumnPosInPk!=null?childColumnPosInPk.intValue():null));
							out.elementf("TD","class='integer'", "%d", 
									(childTotalColumnsInPk!=null?childTotalColumnsInPk.intValue():null));
							
							out.elementf("TD","class='centered'", "%s",
									(parentColumnPosInPk != null && childColumnPosInPk != null &&
									 parentColumnPosInPk.intValue()>0 && childColumnPosInPk.intValue()>0 ? "Y" : null));
						}

						out.elementf("TD","class='centered'", "%s", rs.getObject("parent_pk_only_pair"));
						out.elementf("TD","class='centered'", "%s", rs.getObject("child_pk_only_pair"));
						
						//Link Id
						out.elementf("TD","class='integer'", "%d", rs.getObject("link_id"));
						//Reversal Link Id
						out.elementf("TD","class='integer'", "%d", rs.getObject("rev_link_id"));

						out.write("</TR>\n");

					}
					out.write("</TABLE>\n");
					out.write("<div class='modal'><div id='chart' class='modal-content'></div></div>\n");
					out.write("<script type=\"text/javascript\"> \n");
					out.write(" var modal = document.getElementsByClassName('modal')[0];"
					  +" window.onclick = function(event) { \n"
					  +"  if (event.target == modal) \n"
					  +"      modal.style.display = 'none';\n"
					  +"}\n");
					for(Long columnId : bucketColumnIds) {
						int hitsCounter = 0;
						try( PreparedStatement psb = conn.prepareStatement(
								"select  \n"
								+ "  dc.name as database_name \n"
								+ "  , t.schema_name as schema_name \n" 
								+ "  , t.name as table_name \n"
								+ "  , c.name as column_name \n"
								+ "  , b.bucket_width \n"
								+ "  , b.bucket_no, b.hits \n" 
								+ " from (select p.* \n"
								+ "      ,(select min(bucket_width)  from column_numeric_bucket b where p.col = b.column_id) as min_width \n"
								+ " from (select cast(? as bigint) as col) p \n"  
								+ " ) p2 \n"
								+ " inner join column_info c on c.id = p2.col \n"
								+ " inner join table_info t on t.id = c.table_info_id \n"
								+ " inner join metadata m on m.id  = t.metadata_id \n"
								+ " inner join database_config dc on dc.id  = m.database_config_id\n"
								+ " inner join column_numeric_bucket b on b.column_id = p2.col and b.bucket_width = p2.min_width \n"
								//+ " inner join column_numeric_stats st on st.column_id = p2.col \n"
								+ " order by bucket_no");){
							psb.setLong(1, columnId); 
							try (ResultSet rsb = psb.executeQuery();) {
								while (rsb.next()) {
									if (hitsCounter == 0 ) {
										out.write("buckets");
										out.write(columnId.toString());
										out.write(" = function () {\n");
										out.write(" return {\n");
										out.write(String.format(" entry_name:'%s:%s.%s.%s',\n"
												,rsb.getString("database_name")
												,rsb.getString("schema_name")
												,rsb.getString("table_name")
												,rsb.getString("column_name")
												));
										out.write(String.format(" bucket_width:%d,\n"
												,rsb.getLong("bucket_width")
												));
										out.write(" buckets: [\n");
									}
									hitsCounter++;
									out.write(String.format("    {bucket_no:%d, hits:%d},\n"
											,rsb.getLong("bucket_no")
											,rsb.getLong("hits")
											));
								}
								if (hitsCounter >0 ) {
									out.write("    ]\n  };\n }\n");
								}
								
							}
							
						}
					}
					out.write("</script>\n");
					out.write("</BODY>\n");
					out.write("</HTML>\n");
				}
			}
			System.out.printf("Report has been successfuly written to file %s \n",outFile);
	}
	
	
	
	
	private static class HTMLFileWriter extends FileWriter {

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
	
	private static class TableInfo{
		Long id;
		String schemaName;
		String tableName;
		String pathToFile;
		List<ColumnInfo> columns;
	}

	private static class ColumnInfo{
		Long id;
		Long tableId;
		String colunmName;
		Long position;
		Boolean hasNumericContent;
		Boolean hasFloatContent;
		String minSValue;
		String maxSValue;
		Double minFValue;
		Double maxFValue;
		Long   integerUniqueCount;
		Double movingMean;
		Double standardDeviation;
		Long   positionInPk;
		Long   totalInPk;
		double auxMinFValue,auxMaxFValue;
		Map<Long,SparseBitSet> positiveBitsets;
		Map<Long,SparseBitSet> negativeBitsets;
	}
	
	
	/*
	private static class ColumnStats {
		BigDecimal columnId;
		Boolean isSequence;
		BigDecimal numMin;
		BigDecimal numMax;
		BigDecimal positionInPk;
		BigDecimal totalInPk;
		BigDecimal movingMean;
		BigDecimal stdDev;
		BigDecimal median;
	}*/
	
	private static class H2Repository {
		
	  public TableInfo loadTable(Long tableId) throws SQLException {
		TableInfo result = null;
		if (tableId == null) throw new NullPointerException("tableId is empty");
		
		try(PreparedStatement ps = conn.prepareStatement(
					"select schema_name, name, path_to_file "
					+ " from table_info "
					+ " where id = ?")) {
			ps.setLong(1, tableId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					result = new TableInfo();
					result.schemaName = rs.getString("schema_name");
					result.tableName = rs.getString("name");
					result.pathToFile = rs.getString("path_to_file");
					result.id = tableId;
				}
			}
		}
	 	return result;
	  }
	  public List<ColumnInfo> loadTableColumns(Long tableId) throws SQLException {
		  	List<ColumnInfo> result = null;
			if (tableId == null) throw new NullPointerException("tableId is empty");
			
			try(PreparedStatement ps = conn.prepareStatement(
						"select id, name, position "
						+ " from column_info c  "
						+ " where table_info_id = ? "
						+ " order by c.position")) {
				ps.setLong(1, tableId);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						if (result == null) {
							result = new ArrayList<ColumnInfo>();
						}
						ColumnInfo column = new ColumnInfo();
						column.id = rs.getLong("id");
						column.colunmName = rs.getString("name");
						column.position = rs.getLong("position");
						column.tableId = tableId;
						result.add(column);
					}
				}
			}
		 	return result;
		  }
	  	public void saveColumnStats(ColumnInfo column) throws SQLException {
	  		try (PreparedStatement ps = conn.prepareStatement(
	  				"merge into column_info("
	  				+ " id, has_numeric_content, has_float_content,"
	  				+ " min_fval,max_fval,min_sval,max_sval,"
	  				+ " integer_unique_count,moving_mean,moving_stddev,"
	  				+ " position_in_pk, total_in_pk) key(id) values("
	  				+ " ?,?,?,"
	  				+ " ?,?,substr(?,1,4000),substr(?,1,4000),"
	  				+ " ?,?,?,"
	  				+ " (select cnc.position_in_constraint "
	  				+ "    from constraint_column_info cnc"
	  				+ "     inner join constraint_info cn "
	  				+ "       on cn.id = cnc.constraint_info_id "
	  				+ "      and cn.constraint_type='PK'"
	  				+ "    where cnc.child_column_id = ?),"
	  				+ " (select (select count(*) from constraint_column_info cna where cna.constraint_info_id = cn.id) "
	  				+ "    from constraint_column_info cnc"
	  				+ "     inner join constraint_info cn "
	  				+ "       on cn.id = cnc.constraint_info_id "
	  				+ "      and cn.constraint_type='PK'"
	  				+ "    where cnc.child_column_id = ?) "
	  				+ " )")) {
	  			int col = 0;
	  			ps.setLong(++col, column.id);
	  			ps.setObject(++col, column.hasNumericContent);
	  			ps.setObject(++col, column.hasFloatContent);
	  			ps.setObject(++col, column.minFValue);
	  			ps.setObject(++col, column.maxFValue);
	  			ps.setString(++col, column.minSValue);
	  			ps.setString(++col, column.maxSValue);
	  			ps.setObject(++col, column.integerUniqueCount);
	  			ps.setObject(++col, column.movingMean);
	  			ps.setObject(++col, column.standardDeviation);
	  			ps.setLong(++col, column.id);
	  			ps.setLong(++col, column.id);
	  			ps.executeUpdate();
	  		}
		  	conn.commit();
	  	}
	}
	
}
