/*
 The purpose of this logic is to show how I would get transaction records with information on the filer of the transaction regardless of if it was by a committee or candidate
*/
SELECT 
    indv.tran_id,
    indv.cmte_id AS filer_id,
    CASE
      WHEN can.cand_id IS NOT NULL THEN 'Candidate' 
      WHEN cm.cmte_id IS NOT NULL THEN 'Committee' 
      ELSE 'Unknown'
      END AS filer_type,
    COALESCE(can.cand_state,cm.cmte_st) filer_state,
    COALESCE(can.cand_name,cm.cmte_nm) filer_name
FROM
  (SELECT
      DISTINCT
      cmte_id,
      tran_id
    FROM `bigquery-public-data.fec.indiv20` 
    ) indv
    LEFT JOIN 
    -- Candidates
    (SELECT
      DISTINCT
      cand_id,
      cand_name,
      CASE WHEN cand_office_st IN("US","") OR cand_office_st IS NULL THEN cand_st ELSE cand_office_st END AS cand_state
    FROM `bigquery-public-data.fec.cn20` 
    WHERE cand_office = 'S'
    ) can 
    ON indv.cmte_id = can.cand_id   
    LEFT JOIN
    -- Committees 
    (SELECT
      DISTINCT
        cmte_id,
        cmte_nm,
        tres_nm,
        cmte_st,
    FROM `bigquery-public-data.fec.cm20`
    ) cm
    ON indv.cmte_id = cm.cmte_id
;

 
