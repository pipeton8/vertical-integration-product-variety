/*******************************************************************************
Vertical integration and product variety - Process publishers
Created: August, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Create folders for figures
*******************************************************************************

!mkdir "figures/motivating facts"

*******************************************************************************
** Prepare CPI data to adjust prices
*******************************************************************************

** Import
import delimited "data/raw/cpi_data.csv", clear
compress

** Rename variables
rename observation_date date_str
rename cpiaucsl cpi

** Parse date
gen date = date(date_str, "YMD")
format date %td
drop date_str

** Keep only relevant observations
keep if month(date) == 12 & year(date) >= 2007

** Compute yearly inflation factors
gen year = year(date)
tsset year
gen inflation_factor = F.cpi / cpi

** Compute total inflation factor up to 2024
gen reverse_year = -year
sort reverse_year
gen total_inflation_factor = exp(sum(ln(inflation_factor)))

** Keep relevant variables
keep year total_inflation_factor

** Save
tempfile inflation_factors
save `inflation_factors', replace

*******************************************************************************
** Prepare acquisitions data
*******************************************************************************

** Import
import delimited "data/raw/acquisitions.csv", clear
compress

** Set zero value for one acquisition which was joint
replace nominal_value = 0 if publisher == "Electronic Arts" & developer == "BioWare"

** Count acquisitions and total value
gen total_acquisitions = 1
collapse (sum) total_acquisitions (sum) total_nominal_value = nominal_value, by(acquisition_date)
rename acquisition_date year

** Merge with inflation factors
merge 1:1 year using `inflation_factors'
keep if _merge==3
drop _merge

** Compute real values
gen total_real_value = total_nominal_value * total_inflation_factor / 1000
keep total_acquisitions year total_real_value

** Compute average value per acquisition
gen avg_real_value = total_real_value / total_acquisitions

*******************************************************************************
** Create figure
*******************************************************************************

** Colors
colorpalette reds, select(6) locals(red)
colorpalette reds, select(6) locals(red50) opacity(50)
colorpalette blues, select(7) locals(blue)
colorpalette blues, select(7) locals(blue50) opacity(50)

** Total acquisitions
twoway 	(bar total_acquisitions year, barwidth(1) fcolor(`blue50') lcolor(`blue') yaxis(1)),	///
	xlabel(2007(2)2025, labsize(12pt) angle(30) labgap(5pt))									///
	xscale(range(2006 2025))																	///
	ylabel(0(1)4, labsize(16pt) angle(h) grid)													///
	xtitle("Year", size(16pt) margin(t=2))														///
	ytitle("Number of Acquired Studios", size(16pt))											//

graph export "figures/motivating facts/acquisitions-timeline-number.pdf", replace
graph close

** Total real value
twoway 	(bar total_real_value year, barwidth(1) fcolor(`red50') lcolor(`red') yaxis(1)),	///
	xlabel(2007(2)2025, labsize(12pt) angle(30) labgap(5pt))								///
	xscale(range(2006 2025))																///
	ylabel(0 "0" 0.5 "0.5" 1 "1" 1.5 "1.5" 2 "2", labsize(16pt) angle(h) grid)				///
	xtitle("Year", size(16pt) margin(t=2))													///
	ytitle("Total Value (billions of 2024 USD)", size(14pt))								//

graph export "figures/motivating facts/acquisitions-timeline-totalValue.pdf", replace
graph close

** Average real value
twoway 	(bar avg_real_value year, barwidth(1) fcolor(`red50') lcolor(`red') yaxis(1)),			///
	xlabel(2007(2)2025, labsize(12pt) angle(30) labgap(5pt))									///
	xscale(range(2006 2025))																	///
	ylabel(0 "0" 0.2 "0.2" 0.4 "0.4" 0.6 "0.6" 0.8 "0.8" 1 "1", labsize(16pt) angle(h) grid)	///
	xtitle("Year", size(16pt) margin(t=2))														///
	ytitle("Average Value (billions of 2024 USD)", size(14pt))									//

graph export "figures/motivating facts/acquisitions-timeline-avgValue.pdf", replace
graph close