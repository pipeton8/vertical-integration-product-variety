/*******************************************************************************
Vertical integration and product variety - Master file
Created: August, 2025
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

** Directory change
cd "/Users/pipeton8/Library/CloudStorage/Dropbox/Research/__current/Vertical integration and product variety"

*******************************************************************************
** Process developer and publisher data
*******************************************************************************

include "scripts/1a_process_developers.do"
include "scripts/1b_process_publishers.do"

*******************************************************************************
** Obtain summary statistics and data visualizations
*******************************************************************************

include "scripts/2b_developer_summary_statistics.do"
include "scripts/2c_publisher_summary_statistics.do"
include "scripts/2d_publisher_developer_specialization.do"

*******************************************************************************
** Compute specialization measures and regressions
*******************************************************************************

include "scripts/3a_compute_specializations.do"
include "scripts/3b_genre_similarity_timelines.do"
include "scripts/3c_specialization_regressions.do"
// include "scripts/3d_developer_characteristics_distribution.do"

*******************************************************************************
** Post acquisition quality analysis
*******************************************************************************


