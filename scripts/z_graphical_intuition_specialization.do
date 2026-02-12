/*******************************************************************************
Vertical integration and product variety - Graphical intuition specialization
Created: November, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Preamble
*******************************************************************************

clear all
program drop _all

set more	 off
set varabbrev off
set scheme s1mono

local graphfont "Palatino"
graph set eps fontface `graphfont'
graph set eps fontfaceserif `graphfont'
graph set eps  /*echo back preferences*/

graph set window fontface `graphfont'
graph set window fontfaceserif `graphfont'
graph set window /*echo back preferences*/

*******************************************************************************
** Change directory and global macros
*******************************************************************************

cd "/Users/pipeton8/Library/CloudStorage/Dropbox/Research/__current/Vertical integration and product variety"

!mkdir "figures/specialization intuition"

*******************************************************************************
** Create data
*******************************************************************************

** Set seed and number of observations
set seed 12345
set obs 250

** Generate random x and y coordinates
gen x1 = runiform()
gen x2 = runiform()

** Generate integration status
gen integrated_specialized = 0
replace integrated_specialized = 1 if (x1-0.5)^2 + (x2-0.5)^2 < 0.05

gen integrated_diversified = 0
replace integrated_diversified = 1 if (x1 - x2 > 0.75) | (x2 + x1 > 1.75) | (x1 - x2 < -0.75) | (x2 + x1 < 0.2)

*******************************************************************************
** Plot
*******************************************************************************

** Colors
colorpalette Blues, select(6) locals(blue)
colorpalette Reds, select(7) locals(red)

** Specialization plot
twoway 	(scatter x2 x1 if integrated_specialized == ., msymbol(O) mcolor(`blue') msize(7pt)) 	///
		(scatter x2 x1 if integrated_specialized == 0, msymbol(O) mcolor(`blue') msize(5pt)) 	///
		(scatter x2 x1 if integrated_specialized == 1, msymbol(O) mcolor(`red') msize(7pt)), 	///
	legend(order(1 "Non-Integrated" 3 "Integrated") size(14pt))									///
	xlabel(none) 																				///
	xtitle("Characteristic 1", margin(t=2) size(16pt))											///
	ytitle("Characteristic 2", margin(r=1) size(16pt)) 											///
	ylabel(none) 																				///
	name("specialization_intuition", replace)

** Save graph
graph export "figures/specialization intuition/distribution-specialized.pdf", replace
graph close

** Diversification plot
twoway 	(scatter x2 x1 if integrated_specialized == ., msymbol(O) mcolor(`blue') msize(7pt)) 	///
		(scatter x2 x1 if integrated_diversified == 0, msymbol(O) mcolor(`blue') msize(5pt)) 	///
		(scatter x2 x1 if integrated_diversified == 1, msymbol(O) mcolor(`red') msize(7pt)),	///
	legend(order(1 "Non-Integrated" 3 "Integrated") size(14pt))									///
	xlabel(none) 																				///
	xtitle("Characteristic 1", margin(t=2) size(16pt))											///
	ytitle("Characteristic 2", margin(r=1) size(16pt)) 											///
	ylabel(none) 																				///
	name("diversification_intuition", replace)

** Save graph
graph export "figures/specialization intuition/distribution-diversified.pdf", replace
graph close
