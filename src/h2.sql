create view link_129 as 

create view link_129 as 
select 
   l.id  as id , 
   l.bit_set_exact_similarity as bs,
   l.LUCINE_SAMPLE_TERM_SIMILARITY as  lc,
   l.parent_column_info_id as p_id,
   cast(pi.min_val as float) as p_min,
   cast(pi.max_val as float) as p_max,
   l.child_column_info_id as c_id,
   cast(ci.min_val as float) as c_min,
   cast(ci.max_val as float) as c_max,
   greatest( cast(pi.min_val as float), cast(ci.min_val as float)) as r_min,
   least( cast(pi.max_val as float), cast(ci.max_val as float)) as r_max
   from link l 
     inner join column_info ci on ci.id = l.child_column_info_id
     inner join column_info pi on pi.id = l.parent_column_info_id
   where l.workflow_id = 129 ;



create memory local temporary table ilink as select * from link_129;
create /*memory local temporary*/ table ilink as select * from link_129;


create hash index ilink_pid on ilink(p_id);
create hash index ilink_cid on ilink(c_id);

