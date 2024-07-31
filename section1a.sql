-- Which Senate candidates had the most donors from outside their state in 2020?

-- First I look at the tables I'm using to look for duplicates and NULL
/* Data Quality Check, the Donations table has craaaaaazy duplicates! 
SELECT tran_id,COUNT(1) records, COUNT(DISTINCT transaction_amt) tx_amounts FROM `bigquery-public-data.fec.indiv20` 
GROUP BY 1 HAVING records > 1 LIMIT 200;
SELECT * FROM  `bigquery-public-data.fec.indiv20` WHERE tran_id IN('SA11AI_174518662','2364055','10139616') ORDER BY tran_id; 
*/
-- Data Quality Check: duplicates! 
/*
SELECT cand_pcc,COUNT(1) records, COUNT(DISTINCT cand_id) multiple_cands 
FROM `bigquery-public-data.fec.cn20` 
GROUP BY 1 HAVING records = 1 ORDER BY 2 DESC LIMIT 200;
SELECT * FROM  `bigquery-public-data.fec.cn20` WHERE cand_pcc IN('C00574236','C00746248','C00696872') ORDER BY tran_id; 
*/

-- Now that I have some idea of what I'm up against, I'll do the actual script

CREATE TEMP TABLE out_of_state_contributions AS 
SELECT
    indv.tran_id,
    indv.cmte_id AS filer_id,
    COALESCE(can.state,can_cmt.state) AS filer_state,
    COALESCE(can.cand_name,can_cmt.cand_name) AS candidate,
    indv.unique_donor,
    indv.state AS donor_state,
    indv.transaction_amt,
    PERCENTILE_CONT(indv.transaction_amt, 0.5) OVER (PARTITION BY indv.cmte_id) AS median_cont_to_can,
    SUM(indv.transaction_amt) OVER (PARTITION BY indv.cmte_id, indv.state) AS total_to_can_by_donor_state,
    SUM(indv.transaction_amt) OVER (PARTITION BY indv.state) AS total_cont_by_donor_state,
    -- Take a look at the donations by various donor types
    (CASE WHEN indv.entity_tp ='IND' THEN indv.transaction_amt ELSE 0 END) AS ind_cont,
    (CASE WHEN indv.entity_tp ='PAC' THEN indv.transaction_amt ELSE 0 END) AS pac_cont,
    (CASE WHEN indv.entity_tp ='ORG' THEN indv.transaction_amt ELSE 0 END) AS org_cont,
    (CASE WHEN indv.entity_tp ='CAN' THEN indv.transaction_amt ELSE 0 END) AS can_cont,
    (CASE WHEN indv.entity_tp ='COM' THEN indv.transaction_amt ELSE 0 END) AS com_cont,
    (CASE WHEN indv.entity_tp ='CCM' THEN indv.transaction_amt ELSE 0 END) AS ccm_cont,
    (CASE WHEN indv.entity_tp ='PTY' THEN indv.transaction_amt ELSE 0 END) AS pty_cont,
    (CASE WHEN indv.entity_tp ='' OR indv.entity_tp IS NULL THEN indv.transaction_amt ELSE 0 END) AS unknown_cont
FROM (
    SELECT
      -- the Donations table has craaaaaazy duplicates! Use distinct to reduce duplicates
      DISTINCT 
      state,
      cmte_id,
      tran_id,
      transaction_amt,
      transaction_dt,
      transaction_pgi,
      entity_tp,
      (name || zip_code ||employer ||occupation) AS unique_donor
    FROM `bigquery-public-data.fec.indiv20` 
    ) indv
    -- Match  the filer from the tx to the the candidate 
    LEFT JOIN (
    SELECT
      -- Use distinct to get single records per candidate
      DISTINCT
      cand_id,
      cand_name,
      CASE WHEN cand_office_st IN("US","") OR cand_office_st IS NULL THEN cand_st ELSE cand_office_st END AS state
    FROM `bigquery-public-data.fec.cn20` 
    -- We only want to look at Senate races for this
    WHERE cand_office = 'S'
    ) can
    -- Match  the filer from the tx to the the committee for the candidate 
    -- Some committees are associated with multiple canidates. I'm going to drop those committees. 
    ON (indv.cmte_id = can.cand_id )
    LEFT JOIN (
  SELECT
      DISTINCT
      cand_pcc,
      cand_name,
      CASE WHEN cand_office_st IN("US","") OR cand_office_st IS NULL THEN cand_st ELSE cand_office_st END AS state
    FROM `bigquery-public-data.fec.cn20` 
    WHERE EXISTS (
      SELECT 
        1
      FROM `bigquery-public-data.fec.cn20` 
      GROUP BY 1 
      HAVING COUNT(DISTINCT cand_id) >1 
    )
    AND cand_office = 'S'
    ) can_cmt
    -- Match  the filer from the tx on the committee or candidate 
    ON (indv.cmte_id = can_cmt.cand_pcc)
-- Only donors from outside states
WHERE indv.state != COALESCE(can.state, can_cmt.state)
;



-- SELECT tran_id, COUNT(1) AS records FROM out_of_state_contributions GROUP BY 1 HAVING COUNT(1) >1;
SELECT * FROM out_of_state_contributions WHERE tran_id = '10405668';
SELECT
    filer_id,
    cand_name,
    COUNT(tran_id) as donations,
    COUNT(DISTINCT unique_donor) as donors,
    AVG(transaction_amt) AS avg_cont,
    MAX(median_trans) AS median_cont,
    SUM(transaction_amt) as total_cont,
    -- Take a look at the donations by various donor types
    SUM(CASE WHEN entity_tp ='IND' THEN transaction_amt ELSE 0 END) AS ind_cont,
    SUM(CASE WHEN entity_tp ='PAC' THEN transaction_amt ELSE 0 END) AS pac_cont,
    SUM(CASE WHEN entity_tp ='ORG' THEN transaction_amt ELSE 0 END) AS org_cont,
    SUM(CASE WHEN entity_tp ='CAN' THEN transaction_amt ELSE 0 END) AS can_cont,
    SUM(CASE WHEN entity_tp ='COM' THEN transaction_amt ELSE 0 END) AS com_cont,
    SUM(CASE WHEN entity_tp ='CCM' THEN transaction_amt ELSE 0 END) AS ccm_cont,
    SUM(CASE WHEN ntity_tp ='PTY' THEN transaction_amt ELSE 0 END) AS pty_cont,
    SUM(CASE WHEN entity_tp ='' OR entity_tp IS NULL THEN transaction_amt ELSE 0 END) AS unknown_cont
FROM out_of_state_contributions
GROUP BY 
  filer_id,
  cand_name
ORDER BY donors DESC
;


-- Original Script: which Senate candidates had the most donors from outside their state?
/*
SELECT
    cand_name, cand_office_st, count(*) as donations,
    sum(transaction_amt) as total, COUNT(DISTINCT name || zip_code) as donors
FROM (
SELECT * FROM `bigquery-public-data.fec.indiv20` WHERE entity_tp = "IND"
) c
INNER JOIN (
SELECT * FROM `bigquery-public-data.fec.cn20` WHERE cand_office = 'S'
) can on c.cmte_id = can.cand_pcc
WHERE state != cand_office_st
GROUP BY 1, 2
ORDER BY donors DESC;
*/
