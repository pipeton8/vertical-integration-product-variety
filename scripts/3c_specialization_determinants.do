/*******************************************************************************
Vertical integration and product variety - Determinants of specializations
Created: November, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Create folder for tables
*******************************************************************************

!mkdir "tables/specializations"

*******************************************************************************
** Prepare data
*******************************************************************************

** Open data
use "data/acquisitions_with_specialization.dta", clear

** Compute genre in which developer is most specialized
replace num_games_dev = 0 if missing(num_games_dev)
bysort developer (developer_genre_share positive_share) : gen top_genre_dev = genre[_N]
 
** Save median value
local s_pd_median = strofreal(genre_similarity_median[1], "%5.2f")

*******************************************************************************
** Estimate regresssions
*******************************************************************************

foreach spec in hc median {
	** No fixed effects
	reg specialization_`spec' publisher_num_games_genre games_in_genre if genre == top_genre_dev, vce(robust)

	** Store coefficients
	foreach var in publisher_num_games_genre games_in_genre {
		local var_short = "publisher" * (strpos("`var'", "publisher") > 0) + "market" * (strpos("`var'", "publisher") == 0)
		lincom `var'
		gen b_reg_`var_short'_`spec' = `r(estimate)'
		gen se_reg_`var_short'_`spec'   = `r(se)'
		gen p_reg_`var_short'_`spec'    = `r(p)'
		gen N_reg_`var_short'_`spec'    = `e(N)'
	}

	** With fixed effects
	reghdfe specialization_`spec' publisher_num_games_genre games_in_genre if genre == top_genre_dev, absorb(acquisition_date) vce(robust)

	** Store coefficients
	foreach var in publisher_num_games_genre games_in_genre {
		local var_short = "publisher" * (strpos("`var'", "publisher") > 0) + "market" * (strpos("`var'", "publisher") == 0)

		lincom `var'
		gen b_fe_`var_short'_`spec' = `r(estimate)'
		gen se_fe_`var_short'_`spec'   = `r(se)'
		gen p_fe_`var_short'_`spec'    = `r(p)'
		gen N_fe_`var_short'_`spec'    = `e(N)'
	}
}

*******************************************************************************
** Create table
*******************************************************************************

** Parse results
keep b_* se_* p_* N_*
keep in 1

gen i = 1
reshape long b_ se_ p_ N_, i(i) j(spec) string
drop i
rename *_ *

split spec, parse(_)
order spec*, first
drop spec
rename spec1 spec
rename spec2 var
rename spec3 threshold

// reshape wide b_ se_ p_, i(spec var) j(threshold) string

gen order_spec = (spec == "fe")
gen order_var = (var == "publisher")
gen order_threshold = (threshold == "median")
sort order_threshold order_spec order_var
drop order_*

** Open file
file open specialization_determinants using "tables/specializations/specialization_determinants.tex", write all replace 

** Write header
file write specialization_determinants "\begin{table}" _n
file write specialization_determinants "	\centering" _n
file write specialization_determinants "	\begin{tabular}{lccccc}" _n
file write specialization_determinants "		\hline\hline" _n
file write specialization_determinants "		\\[-1ex]" _n
file write specialization_determinants "			&	\multicolumn{5}{c}{Specialization}		\\[0.5ex] \cline{2-6}" _n
file write specialization_determinants "		\\[-1ex]" _n
file write specialization_determinants "            &       \multicolumn{2}{c}{Hard coded (0.5)}    && \multicolumn{2}{c}{Above median (`s_pd_median')} \\[0.5ex]" _n
file write specialization_determinants "            &	(1)	& (2) && (3) & (4)	\\[0.5ex] \hline" _n
file write specialization_determinants "             \\[-1ex]" _n

** Fill table
foreach var in publisher market {
	** Get variable name
	local var_name1 = "\# of games " + "by publisher" * ("`var'" == "publisher") + "in market" * ("`var'" == "market")
	local var_name2 = "in top genre of developer"

	** Initialize line
	local coefLine = "`var_name1'"
	local SEline = " `var_name2'"

	** Loop over coefficients
	foreach threshold in hc median {
		foreach spec in reg fe {
			** Get coefficients
			qui sum b if var == "`var'" & spec == "`spec'" & threshold == "`threshold'"
			local b = strofreal(`r(mean)', "%6.3f")

			qui sum se if var == "`var'" & spec == "`spec'" & threshold == "`threshold'"
			local se = strofreal(`r(mean)', "%6.3f")

			qui sum p if var == "`var'" & spec == "`spec'" & threshold == "`threshold'"
			local stars = "*" * (`r(mean)' < 0.01) 		///
							+ "*" * (`r(mean)' < 0.05) ///
							+ "*" * (`r(mean)' < 0.1)

			** Append to line
			local coefLine = "`coefLine' & `b'\$^{`stars'}\$"
			local SEline   = "`SEline' & \footnotesize{(`se')}"
		}

		local coefLine = "`coefLine'&"
		local SEline   = "`SEline'&"
	}

	** Cut last alignment tab
	local coefLine = substr("`coefLine'", 1, strlen("`coefLine'") - 1)
	local SEline   = substr("`SEline'", 1, strlen("`SEline'") - 1)

	** Write to file
	file write specialization_determinants "`coefLine'	\\[0.25ex]" _n
	file write specialization_determinants "`SEline'	\\[1ex]" _n
}

** Write specifications and number of observations
file write specialization_determinants "		 \hline" _n
file write specialization_determinants "		 \\[-1ex]" _n
file write specialization_determinants "		Acquisition year FE &  & \$\checkmark\$ && & \$\checkmark\$ \\[1ex]" _n

local N_line = "Observations "

foreach threshold in hc median {
	foreach spec in reg fe {
		qui sum N if threshold == "`threshold'" & spec == "`spec'"
		local N = strofreal(`r(mean)', "%9.0f")
		
		local N_line = "`N_line' & `N'"
	}

	local N_line = "`N_line'&"
}

local N_line = substr("`N_line'", 1, strlen("`N_line'") - 1)
file write specialization_determinants "`N_line' \\[1ex]" _n


** Write footer
file write specialization_determinants "		\\[-2ex]" _n
file write specialization_determinants "		\hline \hline" _n
file write specialization_determinants " 		\\[-1ex]" _n
file write specialization_determinants "		 \multicolumn{6}{l}{$^{***}$ \$p<0.01\$, \$^{**}\$ \$p<0.05\$, \$^{*}\$ \$p<0.1\$. Robust standard errors in parentheses.}	" _n
file write specialization_determinants "	\end{tabular}" _n
file write specialization_determinants "\end{table}" _n

** Close file
file close specialization_determinants