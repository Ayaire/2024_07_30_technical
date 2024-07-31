/* Out-of-state individual donations to federal candidates.
    FILTERS
        by candidate name, 
        candidate office (House, Senate, or both), 
        candidate state. 
    METRICS
        number of donors, 
        the number of donations, 
        and the total amount donated to the candidates from out of state. 
    These metrics should be visible in total and also by donor state.
*/
/*################################################################################################################
Please Note that for tools like Tableau and Sisense I would create a table that is NOT grouped because those 
tools have grouping capacity and so you can do a lot with a single ungrouped data source. 
However, for tools like Looker Studio and Looker that are not as versitile with what they can do with source data
 I would group the tables being fed into them. 
################################################################################################################*/

-- Table for Tableau, Sisense, Spotfire, etc. 
-- The table for production would be a permanent table created on a schedule

CREATE TEMP TABLE out_of_state_contributions AS 
SELECT
    indv.tran_id,
    indv.cmte_id AS filer_id,
    COALESCE(can.state,can_cmt.state) AS filer_state,
    COALESCE(can.cand_id,can_cmt.cand_id) AS candidate_id,
    COALESCE(can.cand_name,can_cmt.cand_name) AS candidate,
    COALESCE(can.cand_office,can_cmt.cand_office) AS office,
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
      CASE 
        WHEN cand_office = 'S' THEN 'Senate'
        WHEN cand_office = 'H' THEN 'House'
        WHEN cand_office = 'P' THEN 'President'
        WHEN cand_office = '' OR cand_office IS NULL THEN 'Unknown'
        ELSE cand_office
        END AS cand_office,
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
      cand_id,
      cand_name,
      CASE 
        WHEN cand_office = 'S' THEN 'Senate'
        WHEN cand_office = 'H' THEN 'House'
        WHEN cand_office = 'P' THEN 'President'
        WHEN cand_office = '' OR cand_office IS NULL THEN 'Unknown'
        ELSE cand_office
        END AS cand_office,
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



--  I would create seperate tables for Looker, Looker Studio, etc: 
-- For efficiency and consistency across measurements I would create a transformation pipeline table that combines the data first
 
/* TABBLE #1
Totals by 
    candidate name, 
    candidate office (House, Senate, or both), 
    candidate state
*/
SELECT
    candidate_id,
    candidate,
    office,
    filer_state,
    SUM(transaction_amt) AS donations,
    COUNT(DISTINCT unique_donor) AS donors,
    SUM(transaction_amt) AS contributions,
    -- Take a look at the donations by various donor types
    SUM(ind_cont) AS ind_cont,
    SUM(pac_cont) AS pac_cont,
    SUM(org_cont) AS org_cont,
    SUM(can_cont) AS can_cont,
    SUM(com_cont) AS com_cont,
    SUM(ccm_cont) AS ccm_cont,
    SUM(pty_cont) AS pty_cont,
    SUM(unknown_cont) AS unknown_cont
FROM out_of_state_contributions
GROUP BY 
    candidate_id,
    candidate,
    office,
    filer_state
;

/* TABBLE #2
Includes aggregation by donor state
*/
SELECT
    candidate_id,
    candidate,
    office,
    filer_state,
    donor_state,
    COUNT(DISTINCT unique_donor) AS donors,
    SUM(transaction_amt) AS contributions,
    MAX(median_cont_to_can) AS overall_median_cont_to_can,
    MAX(total_to_can_by_donor_state) AS total_to_can_by_donor_state,
    MAX(total_cont_by_donor_state) AS total_cont_by_donor_state,
    -- Take a look at the donations by various donor types
    SUM(ind_cont) AS ind_cont,
    SUM(pac_cont) AS pac_cont,
    SUM(org_cont) AS org_cont,
    SUM(can_cont) AS can_cont,
    SUM(com_cont) AS com_cont,
    SUM(ccm_cont) AS ccm_cont,
    SUM(pty_cont) AS pty_cont,
    SUM(unknown_cont) AS unknown_cont
FROM out_of_state_contributions
GROUP BY 
    candidate_id,
    candidate,
    office,
    filer_state,
    donor_state
;
