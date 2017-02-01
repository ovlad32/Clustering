package com.rokittech;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.zaxxer.sparsebits.SparseBitSet;


public class ClusteringLauncher {
	// Clustering.jar a --url tcp://52.59.69.151:9090/./data/h2/edm --uid edm --pwd edmedm --wid 57 --label L2 --bl 0.1 --outfile res.html
	//c --url tcp://52.59.69.151:9090/./data/h2/edm --uid edm --pwd edmedm --wid 162 --label L129 --bl 0.1 --bucket 1000 --outfile res.html
	// c --url tcp://52.29.37.253:9090/./data/h2/edm --uid edm --pwd edmedm --wid 290 --label LabelTest --bl 0.01 --outfile res.html
	static Connection conn;
/*
 
			select * from link_clustered_column where workflow_id = 66
			delete from link_clustered_column where workflow_id = 66

			select pci.min_val,pci.max_val,cci.id,cci.min_val,cci.max_val from link l
 inner join column_info cci on cci.id = l.child_column_info_id
 inner join column_info pci on pci.id = l.parent_column_info_id
where workflow_id = 66 and parent_column_info_id = 947


*/
	static final String clusteredColumnTableDefinition = "create table if not exists link_clustered_column(\n"
			+ " column_info_id bigint not null \n" + " ,workflow_id   bigint not null \n"
			+ " ,cluster_number    integer not null \n" + " ,cluster_label varchar(100) not null \n"
			+ " ,processing_order bigint"
			+ " ,constraint link_clustered_col_pk primary key (column_info_id, workflow_id, cluster_number, cluster_label)\n"
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

	static final String initialClusteringQuery = "insert into link_clustered_column"
			+ " (column_info_id,workflow_id,cluster_number,cluster_label) "
			+ "  select  "
			+ "	     t.parent_column_info_id as column_info_id " 
			+ "		     , t.workflow_id   "
			+ "		     , t.cluster_number   "
			+ "		     , t.cluster_label "
			+ "		     from ( "
			+ "		    select top 1   "
			+ "		           count(1) as cnt "  
			+ "		            ,p.*  "
			+ "		           , l.parent_column_info_id "
			+ "		      from link  l   "
			+ "		      cross join (select "
			+ "		          cast(? as  varchar(100)) as cluster_label "            
			+ "		          , cast(? as bigint)   as workflow_id  "
			+ "		          , cast(? as bigint)  as cluster_number    "    
			+ "		          , cast(? as  double) as bitset_level   "
			+ "		          , cast(? as double) as lucene_level     "
			+ "               ) p  "
			+ "		       left outer join link_clustered_column c "
			+ "		           on c.column_info_id in (l.parent_column_info_id,child_column_info_id) "
			+ "		         and c.cluster_label = p.cluster_label "       
			+ "		         and c.workflow_id = p.workflow_id "
			+ "		      where l.workflow_id = p.workflow_id "
			+ "		      and (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null) "
			+ "		      and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null) "
			+ "		      and c.column_info_id is null    " 
			+ "		     group by parent_column_info_id "
			+ "		     having count(1) >1  "
			+ "		     order by cnt desc " 
			+ "		   ) t ";
			

	static final String workingClusteringQuery = "insert into link_clustered_column"
			+ " (column_info_id,workflow_id,cluster_number,cluster_label) \n"
			+ " select  distinct  "
		 	+ " t.column_info_id, "
		 	+ " t.workflow_id, "
		 	+ " t.cluster_number, "
		 	+ " t.cluster_label "    
		 	+ " 	from ( "
		    + "     select  "
		    + "         c.workflow_id   " 
		    + "       , c.cluster_number    "  
		    + "       , c.cluster_label "
		    + "       , case when c.column_info_id = l.child_column_info_id then l.parent_column_info_id else l.child_column_info_id end as column_info_id "
		    + "       ,(select min(cv.max_val)  "
		    + "                from  link_clustered_column tc  "
		    + "                inner join column_info_numeric_range_view cv on cv.id = tc.column_info_id  "
		    + "                where tc.cluster_number = p.cluster_number and tc.cluster_label = p.cluster_label "
		    + "               ) as upperbound "
		    + "       ,(select max(cv.min_val)  "
		    + "                from  link_clustered_column tc  "
		    + "                inner join column_info_numeric_range_view cv on cv.id = tc.column_info_id  "
		    + "                where tc.cluster_number = p.cluster_number and tc.cluster_label = p.cluster_label "
		    + "                ) as lowerbound "
		    + "    from (select            "
		    + "         cast(? as  varchar(100)) as cluster_label " 
		    + "       , cast(? as bigint)   as workflow_id "            
		    + "       , cast(? as bigint)  as cluster_number  "
		    + "       , cast(? as real) as bitset_level   "          
		    + "       , cast(? as real) as lucene_level "
		    + "        ) p      "
		    + "      inner join link_clustered_column c  "     
		    + "       on c.workflow_id = p.workflow_id "
		    + "      and c.cluster_number = p.cluster_number      "
		    + "      and c.cluster_label = p.cluster_label "
		    + "     inner join link l      "
		    + "      on l.workflow_id =  c.workflow_id "
		    + "     and c.column_info_id in (l.parent_column_info_id, l.child_column_info_id) "
		    + " where (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null) "
		    + " and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null) "
		    + " ) t  "
		    + " inner join column_info_numeric_range_view ci " 
		    + "  on ci.id = t.column_info_id "
		    + " left outer join link_clustered_column c  "    
		    + "  on c.column_info_id = t.column_info_id "
		    + " and c.workflow_id  = t.workflow_id     "
		    + " and c.cluster_label = t.cluster_label " 
		    + " where c.column_info_id is null "
		    + " and (ci.is_numeric_type = false or "
		    /*+ "       t.upperbound is null or "
		    + "       t.lowerbound is null or "*/
		    + "      ((ci.min_val < t.upperbound and ci.max_val > t.lowerbound) and rownum <= 1) "                  
		    + "     ) ";			
			
			/*+ " select  "
			+ " 	distinct  "
			+ " 	t.column_info_id, "
			+ " 	t.workflow_id, "
			+ " 	t.cluster_number, "
			+ " 	t.cluster_label    " 
			+ " 	from ( "
			+ "        select " 
			+ "          c.workflow_id      " 
			+ "          , c.cluster_number    "   
			+ "          , c.cluster_label "
			+ "          , case when c.column_info_id = l.child_column_info_id then l.parent_column_info_id else l.child_column_info_id end as column_info_id "
			+ "          , spn.lowerbound "
			+ "          , spn.upperbound "
			+ "      from (select           "
			+ "            cast(? as varchar(100)) as cluster_label "
			+ "          , cast(? as bigint)   as workflow_id            "
			+ "          , cast(? as bigint)  as cluster_number "
			+ "          , cast(? as double) as bitset_level  "          
			+ "          , cast(? as double) as lucene_level " 
			+ "           ) p     "
			+ "        left outer join link_clustered_column spn "
			+ "          on spn.workflow_id = p.workflow_id "
			+ "         and spn.cluster_number = p.cluster_number      "
			+ "         and spn.cluster_label = p.cluster_label "
			+ "         and spn.upperbound is not null "
			+ "        inner join link_clustered_column c   "    
			+ "          on c.workflow_id = p.workflow_id "
			+ "         and c.cluster_number = p.cluster_number      "
			+ "         and c.cluster_label = p.cluster_label "
			+ "        inner join link l      "
			+ "         on l.workflow_id =  c.workflow_id "
			+ "        and c.column_info_id in (l.parent_column_info_id, l.child_column_info_id) "
			+ "  where (l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null) "
			+ "    and (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null) "
			+ " ) t  "
			+ "   left outer join link_clustered_column c    "  
			+ "     on c.COLUMN_INFO_ID = t.column_info_id "
			+ "     and c.workflow_id  = t.workflow_id     "
			+ "     and c.cluster_label = t.cluster_label " 
			+ "  left outer join column_info ci  "
			+ "      on ci.id = t.column_info_id "
			+ "  left outer join COLUMN_NUMERIC_REAL_TYPE r " 
			+ "     on r.real_type = ci.real_type "
			+ " where c.column_info_id is null "
			+ "  and (r.real_type is null or t.upperbound is null or cast(ci.min_val as double) < t.upperbound )";
  **/

	static final String deleteClusteredColumn = "delete from link_clustered_column c\n" + " where c.workflow_id = ?\n"
			+ "   and c.cluster_label = ?";

	static final String deleteClusteredColumnParam = "delete from link_clustered_column_param c\n"
			+ " where c.workflow_id = ?\n" + "   and c.cluster_label = ?";

	static final String insertClusteredColumnParam = "insert into link_clustered_column_param(workflow_id,cluster_label,bitset_level,lucene_level) \n"
			+ " values(?,?,?,?)";

	static final String reportClusteredColumnsQuery = " select distinct\n" 
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
			+ "     ,case when pcs.column_id is not null then pc.min_val end as parent_min \n"  
			+ "     ,case when pcs.column_id is not null then pc.max_val end as parent_max \n"  
		    + "     ,case when pcs.is_sequence then 'Y' end as parent_is_sequence "
			+ "     ,pcs.std_dev          as parent_std_dev \n" 
			+ "     ,pcs.moving_mean      as parent_moving_mean \n"  
			+ "     ,pcs.median           as parent_median \n" 
		    + "     ,cc.real_type         as child_real_type \n"  
			+ "     ,cc.hash_unique_count as child_huq \n"
			+ "     ,case when ccs.column_id is not null then cc.min_val end as child_min \n"  
			+ "     ,case when ccs.column_id is not null then cc.max_val end as child_max \n"  
		    + "     ,case when ccs.is_sequence then 'Y' end as child_is_sequence "
			+ "     ,ccs.moving_mean      as child_moving_mean \n" 
			+ "     ,ccs.std_dev          as child_std_dev \n" 
			+ "     ,ccs.median           as child_median \n"  
			+ "   from link_clustered_column_param p\n"
			+ "     inner join link_clustered_column c on p.workflow_id = c.workflow_id and p.cluster_label = c.cluster_label\n"
			+ "     inner join link l on c.column_info_id in (l.parent_column_info_id,l.child_column_info_id)\n"
			+ "     inner join column_info pc on pc.id = l.parent_column_info_id "
			+ "     inner join column_info cc on cc.id = l.child_column_info_id "
			+ "     left outer join column_numeric_stats pcs on pcs.column_id = l.parent_column_info_id "
			+ "     left outer join column_numeric_stats ccs on ccs.column_id = l.child_column_info_id "
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
	 "   ,case when pcs.column_id is not null then pc.data_scale end as parent_data_scale \n" + 
	 "   ,case when pcs.column_id is not null then pc.min_val end as parent_min \n" + 
	 "   ,case when pcs.column_id is not null then pc.max_val end as parent_max \n" + 
     "   ,case when pcs.is_sequence then 'Y' end as parent_is_sequence "+
	 "   ,pcs.std_dev          as parent_std_dev \n" + 
	 "   ,pcs.moving_mean      as parent_moving_mean \n" + 
	 "   ,pcs.median           as parent_median \n" + 
	 "   ,pcs.position_in_pk   as parent_position_in_constraint \n"+
	 "   ,pcs.total_in_pk      as parent_total_columns_in_pk\n"+
     "   ,cc.real_type         as child_real_type \n" + 
	 "   ,cc.hash_unique_count as child_huq \n"+ 
	 "   ,case when ccs.column_id is not null then cc.data_scale end as child_data_scale \n" + 
	 "   ,case when ccs.column_id is not null then cc.min_val end as child_min \n" + 
	 "   ,case when ccs.column_id is not null then cc.max_val end as child_max \n" + 
     "   ,case when ccs.is_sequence then 'Y' end as child_is_sequence "+
	 "   ,ccs.moving_mean      as child_moving_mean \n" + 
	 "   ,ccs.std_dev          as child_std_dev \n" + 
	 "   ,ccs.median           as child_median \n" + 
	 "   ,ccs.position_in_pk   as child_position_in_constraint \n"+
	 "   ,ccs.total_in_pk      as child_total_columns_in_pk\n"+
	 "   ,case when pcs.is_sequence = true "+
	 "          and ccs.is_sequence = true "+
	 "          and greatest(pcs.num_max_val, ccs.num_max_val) "+
	 "              - least(pcs.num_min_val, ccs.num_min_val) <> 0 then "+
	 "          1.0*(abs(pcs.num_min_val - ccs.num_min_val) + abs(pcs.num_max_val - ccs.num_max_val)) / "+
	 "            (greatest(pcs.num_max_val, ccs.num_max_val) - "+
	 "              - least(pcs.num_min_val, ccs.num_min_val) ) end as range_similarity" +
	 "   ,(select top 1 'buckets'||b1.column_id from column_numeric_bucket b1 where b1.column_id = l.parent_column_info_id) as parent_buckets \n"+
	 "	 ,(select top 1 'buckets'||b1.column_id from column_numeric_bucket b1 where b1.column_id = l.child_column_info_id) as child_buckets \n"+
	 "   ,l.parent_column_info_id \n" +
	 "   ,cc.table_info_id as child_table_info_id \n"+
	 "   ,l.child_column_info_id \n" + 
	 "   ,pc.table_info_id as parent_table_info_id \n"+
	 "   ,case when pcs.position_in_pk is not null and (\n"+
     "	        select top 1 'Y' from link l1 \n"+
     "	        inner join column_info cc1 on cc1.id = l1.child_column_info_id \n"+
     "	        inner join column_info pc1 on pc1.id = l1.parent_column_info_id \n"+
     "	        where l1.workflow_id = l.workflow_id \n"+
     "	         and cc1.table_info_id = cc.table_info_id \n"+
     "	         and pc1.table_info_id = pc.table_info_id \n"+
     "	         and pc1.id <> l.parent_column_info_id) is null then 'Y' end as parent_pk_only_pair \n"+
	 "   ,case when ccs.position_in_pk is not null and (\n"+
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
	  " left outer join column_numeric_stats pcs on pcs.column_id = l.parent_column_info_id " +
	  " left outer join column_numeric_stats ccs on ccs.column_id = l.child_column_info_id " +
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

	private static final String columnInfoNumericRangeView =
	"CREATE OR REPLACE VIEW PUBLIC.COLUMN_INFO_NUMERIC_RANGE_VIEW AS "
	+ "SELECT "
	+ "  C.ID "
	+ "  ,CASE WHEN (RT.REAL_TYPE IS NOT NULL) THEN TRUE ELSE FALSE END AS IS_NUMERIC_TYPE "
    + "  ,CAST(CASE WHEN (RT.REAL_TYPE IS NOT NULL) THEN C.MIN_VAL END AS DOUBLE) AS MIN_VAL "
    +"   ,CAST(CASE WHEN (RT.REAL_TYPE IS NOT NULL) THEN C.MAX_VAL END AS DOUBLE) AS MAX_VAL "
    +" FROM PUBLIC.COLUMN_INFO C /* PUBLIC.COLUMN_INFO.tableScan */ "
    +" LEFT OUTER JOIN PUBLIC.COLUMN_NUMERIC_REAL_TYPE RT /* PUBLIC.PRIMARY_KEY_4A: REAL_TYPE = C.REAL_TYPE */ "
    +"  ON RT.REAL_TYPE = C.REAL_TYPE ";
    
    
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

				createNumeric2Clusters(parsedArgs.getProperty("label"), longOf(parsedArgs.getProperty("wid")),
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
			} else if ("s".equals(command)) {
				initH2(parsedArgs.getProperty("url"), parsedArgs.getProperty("uid"), parsedArgs.getProperty("pwd"));
				List<String> params = new ArrayList<>();
				params.add("IS_SEQ");
				params.add("MOVING_MEAN");
				if (parsedArgs.getProperty("bucket") != null) {
					params.add("BUCKETS");
				}
				pairStatistics(longOf(parsedArgs.getProperty("wid")), params,parsedArgs);
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
			} else if ("--wid".equals(args[index])) {
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
		System.out.println("   --url  <string>     : URL to ASTRA H2 DB");
		System.out.println("   --uid <string>      : user id for ASTRA H2 DB");
		System.out.println("   --pwd  <string>     : password for ASTRA H2 DB");
		System.out.println("   --wid <integer>     : ASTRA workflow ID to process");
		System.out.println("   --label <string>    : Label name for clustering");
		System.out.println("   --bl <float>        : ASTRA Bitset confidence level of pairs for clustering");
		System.out.println("   --ll <float>        : ASTRA Lucene confidence level of pairs for clustering");
		System.out.println("   --bucket <integer>  : Calculating data buckets with width of <integer>");
		System.out.println("   --outfile <string>  : Output file name");
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

	private static void createClusters(String clusterLabel, Long workflowId, Float bitsetLevel, Float luceneLevel)
			throws SQLException {
		long clusterNo = 0;
		int updated = 0, allUpdated = 0;

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
					clusterNo--;
					// save params;
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
					System.out.println(updated);
					
					if (updated == 0) {
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
	
	
	static void createNumericClusters(String clusterLabel, Long workflowId, Float bitsetLevel, Float luceneLevel) throws SQLException {
		
		
		long clusterNumber = 0;
		long processingOrder = 0;
		
		if (clusterLabel == null || clusterLabel.isEmpty()) {
			throw new RuntimeException("Error: Cluster Label has not been specified!");
		}

		if (workflowId == null) {
			throw new RuntimeException("Error: Workflow ID has not been specified!");
		}

		if (bitsetLevel == null && luceneLevel == null) {
			throw new RuntimeException("Error: Neither Bitset nor Lucene confidence level has not been specified!");
		}

		
		execSQL("drop table if exists t$link");
		execSQL("drop table if exists t$column");
		execSQL("drop table if exists t$param");
		
		
		
		try(PreparedStatement ps = conn.prepareStatement(
				"create /*memory local temporary*/ table t$link as "
						+ " select "
						+ "   l.id  as link_id , "
						+ "   l.bit_set_exact_similarity as bitset_level, "
						+ "   l.lucine_sample_term_similarity as  lucene_level, "
						+ "   l.parent_column_info_id as parent_id, "
						+ "   l.child_column_info_id as child_id, "
						+ "   cast(pi.min_val as double) parent_min_val,"
						+ "   cast(pi.max_val as double) parent_max_val,"
						+ "   cast(ci.min_val as double) child_min_val,"
						+ "   cast(ci.max_val as double) child_max_val,"
						+ "	  greatest( cast(pi.min_val as double), "
						+ "             cast(ci.min_val as double)) as link_min, "
						+ "	  least( cast(pi.max_val as double), "
						+ "          cast(ci.max_val as double)) as link_max "
						+ "	 from link l "
						+ "	     inner join column_info ci on ci.id = l.child_column_info_id "
						+ "	     inner join column_info pi on pi.id = l.parent_column_info_id "
						+ "      inner join column_numeric_real_type rtc on rtc.real_type = ci.real_type"
						+ "      inner join column_numeric_real_type rtp on rtp.real_type = pi.real_type"
						+ "	   where pi.min_val is not null "
						+ "      and pi.max_val is not null "
						+ "      and ci.min_val is not null "
						+ "      and ci.max_val is not null "
						+ "      and l.workflow_id = ?  ")){
			ps.setLong(1, workflowId);
			ps.execute();
		}

		execSQL("create hash index t$link_parent_id on t$link(parent_id)");
		execSQL("create hash index t$link_child_id on t$link(child_id)");
		
		execSQL("create /*memory local temporary*/ table t$column ("
				+ "column_id bigint primary key "
				+ ",cluster_number bigint"
				+ ",processing_order bigint"
				+ ")");
		
		try(PreparedStatement ps = conn.prepareStatement(
				"create /*memory local temporary*/ table t$param as "
				+ "select "
				+ "  cast(? as bigint) workflow_id "
				+ ", cast(? as varchar(100)) as cluster_label "
				+ ", cast(? as double) as bitset_level "
				+ ", cast(? as double) as lucene_level ")){
			ps.setLong(1, workflowId);
			ps.setString(2, clusterLabel);
			ps.setObject(3, bitsetLevel);
			ps.setObject(4, luceneLevel);
			ps.execute();
		};
		
		conn.setAutoCommit(false);

		while (true) {
			Queue<Long> leadingColumnIds = new LinkedList<>();
			Queue<Long> drivenColumnIds = null; 
			BigDecimal clusterMinValue = null, clusterMaxValue = null;
			BigDecimal linkMinValue = null, linkMaxValue = null;
			
			try(Statement initialStm = conn.createStatement();
					ResultSet initialRS = initialStm.executeQuery(
							  "select t.parent_id as column_id  "
							+ "      ,count(*) as  pairs "
							+ "      ,max(t.link_max - t.link_min) as link_delta "
							+ "  from t$link t "
							+ "  inner join t$param p"
							+ "   on ((t.bitset_level >= p.bitset_level or p.bitset_level is null) or "
							+ "       (t.lucene_level >= p.lucene_level or p.lucene_level is null)) "
							+ "  left outer join t$column cp "
							+ "   on cp.column_id = t.parent_id "
							+ "  left outer join t$column cc "
							+ "   on cc.column_id = t.child_id "
							+ " where cp.column_id is null and cc.column_id is null"
							+ " group by t.parent_id "
							+ " having pairs>1"
							+ "union all "
							+ "	select t.child_id as column_id "
							+ "      ,count(*) as  pairs "
							+ "      ,max(t.link_max - t.link_min) as link_delta "
							+ "  from t$link t "
							+ "  inner join t$param p"
							+ "   on ((t.bitset_level >= p.bitset_level or p.bitset_level is null) or "
							+ "       (t.lucene_level >= p.lucene_level or p.lucene_level is null)) "
							+ "  left outer join t$column cp "
							+ "    on cp.column_id = t.parent_id "
							+ "  left outer join t$column cc "
							+ "    on cc.column_id = t.child_id "
							+ " where cp.column_id is null and cc.column_id is null"
							+ " group by t.child_id "
							+ " having pairs>1"
							+ " order by pairs desc, link_delta desc, column_id asc")){
					if (initialRS.next()){
						clusterNumber++;
						processingOrder = 1L;
						Long columnId = initialRS.getLong("column_id");
	
						saveClusteredColumnId(columnId, clusterNumber,processingOrder,null,null);
						leadingColumnIds.offer(columnId);
					} else {
						break;
					}
					
			}
					
			while(!leadingColumnIds.isEmpty()) {
				for(Long leadingColumnId = leadingColumnIds.poll(); 
					leadingColumnId != null;
					leadingColumnId = leadingColumnIds.poll()) {
				
					drivenColumnIds = new LinkedList<>(); 
					try(PreparedStatement ps = conn.prepareStatement(
								"select "
								+ "  t.link_max - t.link_min "
								+ "  ,t.child_id  as column_id"
								+ "  ,t.link_max "
								+ "  ,t.link_min "
								+ "  from t$link t "
								+ "  left outer join t$column c "
								+ "   on c.column_id = t.child_id "
								+ "  where t.parent_id = ? "
								+ "    and c.column_id is null "
								+ " union "
								+ " select "
								+ "  t.link_max - t.link_min "
								+ "  ,t.parent_id as column_id"
								+ "  ,t.link_max "
								+ "  ,t.link_min "
								+ "  from t$link t "
								+ "  left outer join t$column c "
								+ "   on c.column_id = t.parent_id "
								+ "  where t.child_id = ? "
								+ "    and c.column_id is null "
								+ "	order by 1 desc ")) {
						
						ps.setLong(1, leadingColumnId);
						ps.setLong(2, leadingColumnId);
						
						try(ResultSet psrs = ps.executeQuery()) {
							while (psrs.next()) {
								
								linkMinValue = psrs.getBigDecimal("link_min");
								linkMaxValue = psrs.getBigDecimal("link_max");
						
								if (linkMinValue == null || linkMaxValue == null) {
									throw new RuntimeException(String.format(
											"Null(s) in Min/Max! LinkeMin=%f, LinkMax=%f for pair id1 = %d,%d"
											,linkMinValue
											,linkMaxValue
											,leadingColumnId
											,psrs.getLong("column_id"))
											);
								}
								
								if (clusterMinValue == null) {
									clusterMinValue = linkMinValue;
									clusterMaxValue = linkMaxValue;
									drivenColumnIds.offer(psrs.getLong("column_id"));
									continue;
								}

								//!!Here is the place where transitive link filter happens
								if( false || linkMinValue.compareTo(clusterMaxValue) <= 0 && 
								    linkMaxValue.compareTo(clusterMinValue) >= 0 ) {
									
									if(linkMinValue.compareTo(clusterMinValue) > 0 ) clusterMinValue = linkMinValue;
									if(linkMaxValue.compareTo(clusterMaxValue) < 0 ) clusterMaxValue = linkMaxValue;
									
									drivenColumnIds.offer(psrs.getLong("column_id"));
								} else {
									//saveClusteredColumnId(columnId, -1L);
									
								}
							}
						}
					}
				}
			}
			leadingColumnIds = drivenColumnIds;
			//System.out.println(String.format("%d - %d ", clusterNumber, drivenColumnIds.size()));
			if(drivenColumnIds.isEmpty()) 
				break;
			
			for(Long columnId : drivenColumnIds) {
				saveClusteredColumnId(columnId, clusterNumber,++processingOrder,null,null);
			}
	
		}
		
		execSQL("delete from link_clustered_column c "
				+ " where (c.workflow_id,c.cluster_label) = ("
				+ "  select p.workflow_id,p.cluster_label from t$param p "
				+ ")");
		
	 
		//Reoredering cluster numbers and saving collected columns
		boolean updated = false; 
		try(Statement ps = conn.createStatement()){
			updated = 0 != ps.executeUpdate(	
					"insert into link_clustered_column(workflow_id, cluster_label, column_info_id,cluster_number,processing_order) "
							+ " direct "
							+ " select p.workflow_id "
							+ "       ,p.cluster_label "
							+ "       ,t.column_id "
							+ "       ,i.renumbered_cluster_number"
							+ "       ,t.processing_order "
							+ "    from t$param p"
							+ "    cross join ("
							+ "       select "
							+ "         rownum as renumbered_cluster_number,"
							+ "         cluster_number from (	"
							+ "            select ti.cluster_number "
							+ "	         	from t$column ti "
							+ "             where ti.cluster_number>0 "
							+ "		        group by ti.cluster_number "
							+ "		        having count(ti.column_id) >=3 " //a cluster must have 3 or more columns
							+ "           ) "
							+ "        ) i "
							+ "       inner join t$column t "
							+ "    on t.cluster_number = i.cluster_number " 
							+ " ");
		}
		if(updated) {
			execSQL("merge into link_clustered_column_param ( "
					+ " workflow_id "
					+ ",cluster_label "
					+ ",bitset_level "
					+ ",lucene_level "
					+ ") key (workflow_id, cluster_label) "
					+ "select "
					+ " p.workflow_id "
					+ ", p.cluster_label "
					+ ", p.bitset_level "
					+ ", p.lucene_level "
					+ " from t$param p"
					);
			conn.commit();
		}
		conn.rollback();
			
		
	}
	
	static boolean saveClusteredColumnId(Long columnId, Long clusterNumber,Long processingOrder,
			BigDecimal minValue,BigDecimal maxValue) {
		boolean result;
		try(PreparedStatement psu = conn.prepareStatement(
				"merge into t$column(column_id,cluster_number,processing_order,min_val,max_val) "
				+" key(column_id) "
				+" values(?,?,?,?,?)")) {

		psu.setLong(1, columnId);
		psu.setLong(2, clusterNumber);
		psu.setLong(3, processingOrder);
		psu.setBigDecimal(4, minValue);
		psu.setBigDecimal(5, maxValue);
		result = psu.executeUpdate()>0; 
		} catch(SQLException e) {
			throw new RuntimeException(String.format("Exception while saving clustered column column_id=%d",columnId),e);
		}
		return result;
	}
	
	
	static boolean saveChainedColumnId(Long columnId) {
		boolean result;
		try(PreparedStatement psu = conn.prepareStatement(
				"merge into t$chained(column_id,min_val,max_val) "
				+" key(column_id) "
				+" select c.id "
				+" cast(c.min_val as double) as min_val,"
				+" cast(c.max_val as double) as max_val "
				+" from column_info c where c.id = ?")) {

		psu.setLong(1, columnId);
		result = psu.executeUpdate()>0; 
		} catch(SQLException e) {
			throw new RuntimeException(String.format("Exception while saving chained column column_id=%d",columnId),e);
		}
		return result;
	}	
	
	
static void createNumeric2Clusters(String clusterLabel, Long workflowId, Float bitsetLevel, Float luceneLevel) throws SQLException {
		
		long clusterNumber = 0;
		long processingOrder = 0;
		
		if (clusterLabel == null || clusterLabel.isEmpty()) {
			throw new RuntimeException("Error: Cluster Label has not been specified!");
		}

		if (workflowId == null) {
			throw new RuntimeException("Error: Workflow ID has not been specified!");
		}

		if (bitsetLevel == null && luceneLevel == null) {
			throw new RuntimeException("Error: Neither Bitset nor Lucene confidence level has not been specified!");
		}

		
		execSQL("drop table if exists t$link");
		execSQL("drop table if exists t$column");
		execSQL("drop table if exists t$queue");
		execSQL("drop table if exists t$param");
		
		
		try(PreparedStatement ps = conn.prepareStatement(
				"create /*memory local temporary*/ table t$param as "
				+ "select "
				+ "  cast(? as bigint) workflow_id "
				+ ", cast(? as varchar(100)) as cluster_label "
				+ ", cast(? as double) as bitset_level "
				+ ", cast(? as double) as lucene_level ")){
			ps.setLong(1, workflowId);
			ps.setString(2, clusterLabel);
			ps.setObject(3, bitsetLevel);
			ps.setObject(4, luceneLevel);
			ps.execute();
		};

		
		try(PreparedStatement ps = conn.prepareStatement(
				"create /*memory local temporary*/ table t$link as "
						+ " select "
						+ "   l.id  as link_id , "
						+ "   l.bit_set_exact_similarity as bitset_level, "
						+ "   l.lucine_sample_term_similarity as  lucene_level, "
						+ "   l.parent_column_info_id as parent_id, "
						+ "   l.child_column_info_id as child_id, "
						+ "   cast(pi.min_val as double) parent_min_val,"
						+ "   cast(pi.max_val as double) parent_max_val,"
						+ "   cast(ci.min_val as double) child_min_val,"
						+ "   cast(ci.max_val as double) child_max_val,"
						+ "	  greatest( cast(pi.min_val as double), "
						+ "             cast(ci.min_val as double)) as link_min, "
						+ "	  least( cast(pi.max_val as double), "
						+ "          cast(ci.max_val as double)) as link_max "
						+ "	 from link l "
						+ "	     inner join column_info ci on ci.id = l.child_column_info_id "
						+ "	     inner join column_info pi on pi.id = l.parent_column_info_id "
						+ "      inner join column_numeric_real_type rtc on rtc.real_type = ci.real_type"
						+ "      inner join column_numeric_real_type rtp on rtp.real_type = pi.real_type"
						+ "   inner join t$param p"
						+ "    on ((l.bit_set_exact_similarity >= p.bitset_level or p.bitset_level is null) or "
						+ "        (l.lucine_sample_term_similarity >= p.lucene_level or p.lucene_level is null)) "
						+ "	   where pi.min_val is not null "
						+ "      and pi.max_val is not null "
						+ "      and ci.min_val is not null "
						+ "      and ci.max_val is not null "
						+ "      and l.workflow_id = ?  ")){
			ps.setLong(1, workflowId);
			ps.execute();
		}

		execSQL("create hash index t$link_parent_id on t$link(parent_id)");
		execSQL("create hash index t$link_child_id on t$link(child_id)");
		
		execSQL("create /*memory local temporary*/ table t$queue ("
				+ "column_id bigint primary key "
				+ ",min_val double, max_val double )");

		execSQL("create /*memory local temporary*/ table t$column ("
				+ "column_id bigint primary key "
				+ ",cluster_number bigint"
				+ ",processing_order bigint"
				+ ",min_val double, max_val double"
				+ ")");
		
		
		conn.setAutoCommit(true);

		while (true) {
			//Queue<Long> leadingColumnIds = new LinkedList<>();
			//Queue<Long> drivenColumnIds = null;
			execSQL("truncate table t$queue");
			try(PreparedStatement ps = conn.prepareStatement(
							"  merge into t$queue(column_id,min_val,max_val) key(column_id) "
							+ " select top 1 column_id, min_val, max_val "
							+ " from ("
							+ "   select t.parent_id as column_id  "
							+ "    ,count(*) as  pairs "
							+ "    ,c.min_val, c.max_val"
							+ "    ,c.max_val - c.min_val as range_val"
							+ "    from t$link t "
							+ "   inner join column_info_numeric_range_view c "
							+ "     on c.id = t.parent_id "
							+ "   left outer join t$column cp "
							+ "     on cp.column_id = t.parent_id "
							+ "   left outer join t$column cc "
							+ "     on cc.column_id = t.child_id "
							+ "   where cp.column_id is null and cc.column_id is null"
							+ "    group by t.parent_id,c.min_val, c.max_val "
							+ "    having pairs"
							+ "  union "
							+ "	 select t.child_id as column_id "
							+ "    ,count(*) as  pairs "
							+ "    ,c.min_val, c.max_val "
							+ "    ,c.max_val - c.min_val as range_val "
							+ "    from t$link t "
							+ "   inner join column_info_numeric_range_view c "
							+ "     on c.id = t.child_id "
							+ "  left outer join t$column cp "
							+ "    on cp.column_id = t.parent_id "
							+ "  left outer join t$column cc "
							+ "    on cc.column_id = t.child_id "
							+ "  where cp.column_id is null and cc.column_id is null"
							+ "   group by t.child_id,c.min_val, c.max_val "
							+ "   having pairs > 1"
							+ " order by pairs desc, range_val desc"
							+ " )"
							)){
					int count = ps.executeUpdate();
					if ( count == 0) break;
				}
				while(true) {
					try(PreparedStatement ps = conn.prepareStatement(
								" insert into t$queue(column_id,min_val,max_val) "
								+ " select c.id,c.min_val,c.max_val "
								+ " from ( "
								+ " select "
								+ "  t.child_id  as column_id"
								+ "  from t$queue h "
								+ "   inner join t$link t  on t.parent_id = h.column_id"
								+ "  left outer join t$column c "
								+ "   on c.column_id = t.child_id "
								+ "  where c.column_id is null "
								+ " union "
								+ " select "
								+ "  t.parent_id as column_id"
								+ "  from t$queue h "
								+ "   inner join t$link t on t.child_id = h.column_id"
								+ "  left outer join t$column c "
								+ "   on c.column_id = t.parent_id "
								+ "  where c.column_id is null "
								+ " minus "
								+ " select column_id from t$queue"
								+ ") t inner join column_info_numeric_range_view c on c.id = t.column_id ")) {
						
						int count = ps.executeUpdate();
						if ( count == 0) break;
					
					}
				}
		
		
		
				while (true) {
					//Queue<Long> leadingColumnIds = new LinkedList<>();
					//Queue<Long> drivenColumnIds = null; 
					BigDecimal clusterMinValue = null, clusterMaxValue = null,clusterRangeValue = null;
					BigDecimal columnMinValue = null, columnMaxValue = null, columnRangeValue = null;
					
					try(Statement st = conn.createStatement();
							ResultSet rs = st.executeQuery(
									"  select "
									+ " t.column_id "
									+ " ,t.max_val - t.min_val as range_val"
									+ " ,t.min_val, t.max_val"
									+ "  from t$queue t "
									+ "  left outer join t$column c "
									+ "   on c.column_id = t.column_id  "
									+ " where c.column_id is null "
									+ " order by range_val desc ")){
						while(rs.next()) {
							 
							 Long columnId = rs.getLong("column_id");
							 columnRangeValue = rs.getBigDecimal("range_val");
							 columnMinValue = rs.getBigDecimal("min_val");
							 columnMaxValue = rs.getBigDecimal("max_val");
							 
							 if (clusterRangeValue == null) {
									clusterMinValue = columnMinValue;
									clusterMaxValue = columnMaxValue;	
									clusterRangeValue = columnRangeValue;
									processingOrder = 0;
									saveClusteredColumnId(columnId, ++clusterNumber, ++processingOrder,clusterMinValue,clusterMaxValue);
									continue;
								}
							 
							//!!Here is the place where transitive link filter happens
							if( false || columnMinValue.compareTo(clusterMaxValue) <= 0 && 
									columnMaxValue.compareTo(clusterMinValue) >= 0 ) {
								
								if(columnMinValue.compareTo(clusterMinValue) > 0 ) clusterMinValue = columnMinValue;
								if(columnMaxValue.compareTo(clusterMaxValue) < 0 ) clusterMaxValue = columnMaxValue;
								saveClusteredColumnId(columnId, clusterNumber, ++processingOrder,clusterMinValue,clusterMaxValue);
							 
							}
						}
					}
					if (processingOrder <3) {
						break;
					}
				}
				
		}
		
		execSQL("delete from link_clustered_column c "
				+ " where (c.workflow_id,c.cluster_label) = ("
				+ "  select p.workflow_id,p.cluster_label from t$param p "
				+ ")");
		
	 
		//Reoredering cluster numbers and saving collected columns
		boolean updated = false; 
		try(Statement ps = conn.createStatement()){
			updated = 0 != ps.executeUpdate(	
					"insert into link_clustered_column(workflow_id, cluster_label, column_info_id,cluster_number,processing_order) "
							+ " direct "
							+ " select p.workflow_id "
							+ "       ,p.cluster_label "
							+ "       ,t.column_id "
							+ "       ,i.renumbered_cluster_number"
							+ "       ,t.processing_order "
							+ "    from t$param p"
							+ "    cross join ("
							+ "       select "
							+ "         rownum as renumbered_cluster_number,"
							+ "         cluster_number from (	"
							+ "            select ti.cluster_number "
							+ "	         	from t$column ti "
							+ "             where ti.cluster_number>0 "
							+ "		        group by ti.cluster_number "
							+ "		        having count(ti.column_id) >=3 " //a cluster must have 3 or more columns
							+ "           ) "
							+ "        ) i "
							+ "       inner join t$column t "
							+ "    on t.cluster_number = i.cluster_number " 
							+ " ");
		}
		if(updated) {
			execSQL("merge into link_clustered_column_param ( "
					+ " workflow_id "
					+ ",cluster_label "
					+ ",bitset_level "
					+ ",lucene_level "
					+ ") key (workflow_id, cluster_label) "
					+ "select "
					+ " p.workflow_id "
					+ ", p.cluster_label "
					+ ", p.bitset_level "
					+ ", p.lucene_level "
					+ " from t$param p"
					);
			conn.commit();
		}
		conn.rollback();
			
		
	}


	
	
	static final BigDecimal SequenceDeviationThreshold = new BigDecimal(0.03f);
	
	
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
				/*DB bucketDB = DBMaker.fileDB(String.format("./c%d.mapdb",stats.columnId.longValue())).
						fileMmapEnable().make();
				BTreeMap<BigDecimal,BigInteger> map = bucketDB.treeMap("c",Serializer.BIG_DECIMAL,Serializer.BIG_INTEGER).create();
				*/
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
	
	private static void makeTableColStats() throws SQLException {
		execSQL("create table if not exists column_numeric_stats( "
				+ "column_id bigint"
				+ ", moving_mean real"
				+ ", std_dev real"
				+ ", median bigint"
				+ ", is_sequence boolean"
				+ ", num_min_val bigint"
				+ ", num_max_val bigint"
				+ ", position_in_pk bigint"
				+ ", total_in_pk bigint"
				+ ", constraint column_numeric_stats_pk primary key (column_id))");
		
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
				+ " create view if not exists column_info_numeric_range_view as " 
				+ " select c.id "
				+ "      ,case when rt.real_type is not null then true else false end as is_numeric_type "
				+ "      ,cast(case when rt.real_type is not null then c.min_val end as double) as min_val "
				+ "      ,cast(case when rt.real_type is not null then c.max_val end as double) as max_val "
				+ " from column_info c "
				+ "  left outer join column_numeric_real_type rt on rt.real_type = c.real_type "
				);
		execSQL(columnInfoNumericRangeView);
  }
	
	private static ColumnStats getColStats(BigDecimal columnId) throws SQLException {
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
						+ "    ,c.max_val"
						+ "    ,c.min_val"
						+ "    ,c.hash_unique_count"
						+ "    ,nvl(c.data_scale,0) as data_scale "
						+ "    ,cnc.position_in_constraint "
						+ "    ,case when cn.id is not null then ("
						+ "           select count(*) from constraint_column_info c1 "
						+ "           where c1.constraint_info_id = cn.id) end as total_in_pk"
						+ "   from ("
						+ " select parent_column_info_id as id from link l  where l.WORKFLOW_ID = ? "
						+ "   union "
						+ "   select child_column_info_id as id from link l  where l.WORKFLOW_ID = ? "
						+ " ) r inner join COLUMN_INFO_NUMERIC_RANGE_VIEW cv on cv.id = r.id "
						+ "     inner join column_info c on c.id = r.id "
						+ "     left outer join constraint_column_info cnc on cnc.child_column_id = r.id"
						+ "     left outer join constraint_info cn on cn.id = cnc.constraint_info_id and cn.constraint_type='PK'"
						+ " where cv.is_numeric_type=true "
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
					out.elementf("TD","class='confidence'", "%s", rs.getString("PARENT_MIN"));
					out.elementf("TD","class='confidence'", "%s", rs.getString("PARENT_MAX"));
					out.elementf("TD","%s", rs.getString("PARENT_REAL_TYPE"));

					out.elementf("TD","class='centered'", "%s", rs.getString("child_is_sequence")); 
					out.elementf("TD","class='integer'", "%d", rs.getObject("CHILD_HUQ"));
					out.elementf("TD","class='confidence'", "%s", rs.getString("CHILD_MIN"));
					out.elementf("TD","class='confidence'", "%s", rs.getString("CHILD_MAX"));
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
						out.elementf("TD","class='integer'", "%s", rs.getString("parent_min"));
						out.elementf("TD","class='integer'", "%s", rs.getString("parent_max"));
						out.elementf("TD", "class='integer'", "%f",rs.getBigDecimal("parent_median"));
						out.elementf("TD", "class='integer'", "%f",rs.getBigDecimal("parent_moving_mean"));
						out.elementf("TD", "class='confidence'", "%f",rs.getBigDecimal("parent_std_dev"));
						out.element("TD", rs.getString("parent_real_type"));
						

						out.element("TD", "class='centered'", rs.getString("child_is_sequence"));
						out.elementf("TD","class='integer'", "%d", rs.getObject("child_huq"));
						out.elementf("TD","class='integer'", "%s", rs.getString("child_min"));
						out.elementf("TD","class='integer'", "%s", rs.getString("child_max"));
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
		
	}
}