-- Only looking at convictions - so won't pick up pending cases
select DEFLGKY as def_key, TTITLE1 as top_charge, DISP1 as top_disp, TTITLE1, DISP1, PRISTIM1, TTITLE2, PRISTIM2, DISP2, TTITLE3, DISP3, PRISTIM3, TTITLE4, PRISTIM4, DISP4, TTITLE5, PRISTIM5, DISP5
FROM md_base
where CIRCUIT = "04" 
and DISTRICT = "16"
and right (FILEDATE, 4) > 2004
and ( 
( (TTITLE1 like "18%1341%" or TTITLE1 like "18%1343%" or TTITLE1 like "18%1344%" or TTITLE1 like "18%1349%") and DISP1 in ("4", "5", "8", "9"))
or ( (TTITLE2 like "18%1341%" or TTITLE2 like "18%1343%" or TTITLE2 like "18%1344%" or TTITLE2 like "18%1349%") and DISP2 in ("4", "5", "8", "9"))
or ( (TTITLE3 like "18%1341%" or TTITLE3 like "18%1343%" or TTITLE3 like "18%1344%" or TTITLE3 like "18%1349%") and DISP3 in ("4", "5", "8", "9"))
or ( (TTITLE4 like "18%1341%" or TTITLE4 like "18%1343%" or TTITLE4 like "18%1344%" or TTITLE4 like "18%1349%") and DISP4 in ("4", "5", "8", "9"))
or ( (TTITLE5 like "18%1341%" or TTITLE5 like "18%1343%" or TTITLE5 like "18%1344%" or TTITLE5 like "18%1349%") and DISP5 in ("4", "5", "8", "9"))
)
