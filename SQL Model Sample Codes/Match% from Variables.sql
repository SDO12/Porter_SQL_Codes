---There are Drivers who’s Owner PAN is different in GSTN, Enrollment and Drivers table.
---DJEPA5104F (gstn drivers) <> DLEPA5104F (enrollment) → Wrong PAN ENTRY

---Cross check in partner_payments 
---select * from prod_curated.oms_public.partner_owners 
---where mobile in (
---select mobile from prod_curated.oms_public.PAYMENT_OWNERS where upper(PAN_NUMBER) ='DLEPA5104F');

---Code to get match% of 2 variables
Below match code
WITH inputs AS (
  SELECT 
    'DJEPA5104F' AS str1,
    'DLEPA5104F' AS str2
),
positions AS (
  SELECT 
    SEQ4() + 1 AS pos
  FROM TABLE(GENERATOR(ROWCOUNT => 10))  -- hardcoded length pan 10 digit so
),
char_compare AS (
  SELECT 
    pos,
    SUBSTR(str1, pos, 1) AS char1,
    SUBSTR(str2, pos, 1) AS char2
  FROM inputs, positions
),
match_stats AS (
  SELECT
    COUNT(*) AS total_chars,
    COUNT_IF(char1 = char2) AS matching_chars
  FROM char_compare
)
SELECT
  matching_chars,
  total_chars,
  (matching_chars::FLOAT / total_chars) * 100 AS match_percentage
FROM match_stats;

