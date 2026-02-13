/*******************************************************************************
Vertical integration and product variety - Genre similarity timeline
Created: November, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Create folder for figures
*******************************************************************************

!mkdir "figures/specializations"

*******************************************************************************
** Prepare data
*******************************************************************************

** Open data
use "data/acquisitions_with_specialization.dta", clear

*******************************************************************************
** Plot timeline of genre similarity
*******************************************************************************

** Obtain median
local s_pd_median = strofreal(genre_similarity_median[1], "%6.2f")

** Color
colorpalette blues, select(8) local(blue)
colorpalette reds,  select(8) local(red)

** Plot
twoway (scatter genre_similarity acquisition_date, mcolor(`blue') msize(5pt))							///
		if acquisition_tag,																				///
	   xlabel(2007(2)2025, labsize(14pt) labgap(5pt) angle(45))						///
	   ylabel(0(0.2)1, labsize(16pt) grid angle(h)format("%03.1f")) 													///
	   yline(0.5, lpattern(dash) lcolor(`red')) 														///
	   yline(`=genre_similarity_median[1]', lpattern(dash) lcolor(dkgreen))	 												///
	   text(0.56 2006.75 "{it:s{sub:pd}} = 0.5", size(12pt) color(`red') placement(east))				///
	   text(`=genre_similarity_median[1]+0.06' 2006.75 "{it:s{sub:pd}} = `s_pd_median'", size(12pt) color(dkgreen) placement(east))	///
	   ytitle("Genre similarity", size(16pt) ) 															///
	   xtitle("Year", size(16pt) ) 																		//

graph export "figures/specializations/genre_similarity_timeline.pdf", replace
graph close