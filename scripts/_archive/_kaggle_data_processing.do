/*******************************************************************************
Vertical integration and product variety - Kaggle data processing
Created: October, 2025
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

*******************************************************************************
** Process videogame developers data
*******************************************************************************

** Load
import delimited "data/raw/videogame_developers_kaggle.csv", clear varnames(1)

** Keep relevant variables
keep developer est notes

** Destring est
destring est, generate(year_established) force

** Fix developer names
replace developer = "Double Helix Games" 	if developer == "Amazon Game Studios, Orange County (formerly Double Helix Games)"
replace developer = "Bandai Namco" 			if developer == "Bandai Namco EntertainmentBandai Namco Studios"
replace developer = "Cyberlore Studios" 	if developer == "Cyberlore Studios (Blueline Simulations[9])"
replace developer = "Digital Illusions"		if developer == "EA Digital Illusions CE (EA DICE)"
replace developer = "Distinctive Software"	if developer == "EA Vancouver (formerly Distinctive Software)"
replace developer = "Koei Tecmo"			if developer == "Koei Tecmo Games (formerly Koei)"
replace developer = "Turbine"				if developer == "WB Games Boston (formerly Turbine)"
replace developer = "Avalanche Software"	if developer == "WB Games - Avalanche (formerly Avalanche Software)"
replace developer = "Bend Studio" 			if developer == "SIE Bend Studio"
replace developer = "Iguana Entertainment" 	if developer == "Acclaim Studios Austin"

replace developer = "Nexon" 				if developer == "NexonNexon Korea"
replace developer = "Wizet Studio"			if developer == "Wizet studio (Nexon development 1st division)"
replace developer = "devCAT Studios" 		if developer == "devCAT Studios (Nexon development 3rd division)"

** Fix founded year for some developers
replace year_established = 2005 if developer == "4A Games"
replace year_established = 1988 if developer == "Aces Studio"
replace year_established = 1955 if developer == "Bandai Namco"
replace year_established = 1984 if developer == "The Dovetail Group"
replace year_established = 1994 if developer == "Nexon"
replace year_established = 1976 if developer == "Red Entertainment"
replace year_established = 1998 if developer == "Rockstar Games"
replace year_established = 1993 if developer == "Bend Studio"
replace year_established = 1997 if developer == "Guerrilla Cambridge"
replace year_established = 1989 if developer == "ToeJam & Earl Productions"

** Add source variable
gen source = "videogame_developers_kaggle"

* Save
compress
save "data/processed_videogame_developers_kaggle.dta", replace

*******************************************************************************
** Process indie developers data
*******************************************************************************

** Load
import delimited "data/raw/indie_developers_kaggle.csv", clear varnames(1)

** Keep relevant variables
keep developer notes

** Add founded year (TODO)
gen year_established = .
order year_established, after(developer)

** Fix developer names
replace developer = "Capybara Games" 				if developer == "Capybara Games[1][2]"
replace developer = "Daniel Benmergui" 				if developer == "Daniel Benmergui[2]"
replace developer = "Davilex Games" 				if developer == "Davilex Games[2]"
replace developer = "Dejobaan Games" 				if developer == "Dejobaan Games[1]"
replace developer = "Derek Yu" 						if developer == "Derek Yu[2]"
replace developer = "Dr. Panda" 					if developer == "Dr. Panda[3]"
replace developer = "Edmund McMillen" 				if developer == "Edmund McMillen[2]"
replace developer = "Hello Games" 					if developer == "Hello Games[1]"
replace developer = "Jason Rohrer" 					if developer == "Jason Rohrer[1]"
replace developer = `"Jonatan "Cactus" Söderström"'	if developer == `"Jonatan "Cactus" Söderström[1]"'
replace developer = "Klei Entertainment"			if developer == "Klei Entertainment[1]"
replace developer = "Kloonigames"					if developer == "Kloonigames[1]"

** Add source variable
gen source = "indie_developers_kaggle"

* Save
compress
save "data/processed_indie_developers_kaggle.dta", replace

*******************************************************************************
** Process videogame sales data
*******************************************************************************

** Load
import delimited "data/raw/videogame_sales_kaggle.csv", clear varnames(1)

** Keep relevant variables
keep developer 

** Drop missings and duplicates
drop if missing(developer)
duplicates drop

** Some games have multiple developers separated by ,
split developer, parse(,) gen(dev_temp)
drop developer

gen foo = _n
reshape long dev_temp, i(foo) j(dev_number)
drop foo dev_number
rename dev_temp developer
replace developer = strtrim(developer)

drop if missing(developer)
duplicates drop

** Drop some names that came from "Ltd.", "Inc.", etc.
drop if inlist(developer, "Inc", "Inc.", "LLC", "Ltd.", "Lda")

** Change some developer names
replace developer = "Eighting" 		if developer == "Eighting/Raizing"
replace developer = "Interchannel" 	if developer == "Interchannel-Holon"

** Add source variable
gen source = "videogame_sales_kaggle"

** Save
compress
save "data/developers_from_sales_kaggle.dta", replace

*******************************************************************************
** Combine datasets
*******************************************************************************

** Load processed datasets
use "data/processed_videogame_developers_kaggle.dta", clear
append using "data/processed_indie_developers_kaggle.dta"
append using "data/developers_from_sales_kaggle.dta"

** Keep only relevant variables
keep developer year_established source notes

** Sort by developer
sort developer

** Drop duplicates
duplicates drop developer, force

gen developer_lc = lower(developer)
sort developer_lc

** Fix some duplicates that are called differently across datasets
replace developer = "5th Cell" 					if inlist(developer, "5TH Cell", "5th Cell")
replace developer = "Art Co., Ltd" 					if inlist(developer, "Art", "Art Co.", "Art Co., Ltd")
replace developer = "Ascaron Entertainment" 		if inlist(developer, "Ascaron Entertainment GmbH", "Ascaron Entertainment")
replace developer = "Atari" 						if inlist(developer, "Atari SA", "Atari")
replace developer = "Atlus" 						if inlist(developer, "Atlus Co.", "Atlus")
replace developer = "Avalanche Studios" 			if inlist(developer, "Avalanche Studios Group", "Avalanche Studios")
replace developer = "B.B. Studio" 					if inlist(developer, "B.B.Studio", "B.B. Studio")
replace developer = "Bandai Namco" 					if inlist(developer, "Bandai", "Bandai Namco", "Bandai Namco Games")
replace developer = "Big Blue Bubble" 				if inlist(developer, "Big Blue Bubble Inc.", "Big Blue Bubble")
replace developer = "Blue Tongue Entertainment" 	if inlist(developer, "Blue Tongue", "Blue Tongue Entertainment")
replace developer = "Bluehole Studio" 				if inlist(developer, "Bluehole", "Bluehole Studio")
replace developer = "Boomzap Entertainment" 		if inlist(developer, "Boomzap", "Boomzap Entertainment")
replace developer = "Buena Vista Games" 			if inlist(developer, "Buena Vista Interactive", "Buena Vista Games")
replace developer = "CCP Games" 					if inlist(developer, "CCP", "CCP Games")
replace developer = "CD Projekt RED" 				if inlist(developer, "CD Projekt Red", "CD Projekt Localisation Centre", "CD Projekt Red Studio", "CD Projekt")
replace developer = "Cauldron" 						if inlist(developer, "Cauldron", "Cauldron Ltd.")
replace developer = "Cavia" 						if inlist(developer, "Cavia Inc.", "Cavia")
replace developer = "ChunSoft" 						if inlist(developer, "ChunSoft", "Chunsoft")
replace developer = "Comcept" 						if inlist(developer, "comcept", "Comcept")
replace developer = "Core Design" 					if inlist(developer, "Core Design Ltd.", "Core Design")
replace developer = "Creatures Inc." 				if inlist(developer, "Creatures", "Creatures Inc.")
replace developer = "Cyanide Studios" 				if inlist(developer, "Cyanide", "Cyanide Studios")
replace developer = "CyberPlanet Interactive" 		if inlist(developer, "Cyber Planet", "CyberPlanet Interactive Public Co.")
replace developer = "Danger Close Games" 			if inlist(developer, "Danger Close", "Danger Close Games")
replace developer = "Deck13" 						if inlist(developer, "Deck 13", "Deck13")
replace developer = "Dimps" 						if inlist(developer, "Dimps Corporation", "Dimps")
replace developer = "Disney Interactive Studios" 	if inlist(developer, "Disney Interactive", "Disney Interactive Studios")
replace developer = "Dontnod Entertainment" 		if inlist(developer, "DONTNOD Entertainment", "Dontnod Entertainment")
replace developer = "DreamCatcher Interactive" 		if inlist(developer, "Dreamcatcher", "DreamCatcher Interactive")
replace developer = "DreamWorks Interactive" 		if inlist(developer, "Dreamworks Games", "Dreamworks Interactive")
replace developer = "Eden Games" 					if inlist(developer, "Eden", "Eden Games", "Eden Studios")
replace developer = "EKO Software" 					if inlist(developer, "EKO Software", "Eko Software")
replace developer = "Epicenter Studios" 			if inlist(developer, "Epicenter Interactive", "Epicenter Studios")
replace developer = "Evolution Studios" 			if inlist(developer, "Evolution Games", "Evolution Studios")
replace developer = "FarSight Studios" 				if inlist(developer, "Farsight Technologies", "Farsight Studios", "FarSight Studios")
replace developer = "Fatshark" 						if inlist(developer, "Fatshark AB", "Fatshark")
replace developer = "FeelPlus" 						if inlist(developer, "feelplus", "FeelPlus")
replace developer = "Firefly Studios" 				if inlist(developer, "FireFly Studios", "Firefly Studios")
replace developer = "Frima Studio" 					if inlist(developer, "Frima", "Frima Studio")
replace developer = "FromSoftware" 					if inlist(developer, "From Software", "FromSoftware")
replace developer = "Full Fat" 						if inlist(developer, "Full Fat", "Full-Fat")
replace developer = "Funatics Software" 			if inlist(developer, "Funatics", "Funatics Software")
replace developer = "Gaijin Entertainment" 			if inlist(developer, "Gaijin", "Gaijin Entertainment")
replace developer = "Genius Sonority" 				if inlist(developer, "Genius Sonority Inc.", "Genius Sonority")
replace developer = "Grezzo" 						if inlist(developer, "GREZZO", "Grezzo")
replace developer = "Guerrilla Cambridge" 			if inlist(developer, "Guerilla Cambridge", "Guerrilla Cambridge")
replace developer = "Guerrilla Games" 				if inlist(developer, "Guerrilla", "Guerrilla Games")
replace developer = "GungHo Online Entertainment" 	if inlist(developer, "GungHo", "GungHo Online Entertainment")
replace developer = "h.a.n.d. Inc." 				if inlist(developer, "h.a.n.d.", "h.a.n.d. Inc.")
replace developer = "Haemimont Games" 				if inlist(developer, "Haemimont", "Haemimont Games")
replace developer = "HAL Laboratory" 				if inlist(developer, "HAL Labs", "HAL Laboratory")
replace developer = "Halfbrick Studios" 			if inlist(developer, "Halfbrick", "Halfbrick Studios")
replace developer = "HB Studios" 					if inlist(developer, "HB Studios Multimedia", "HB Studios")
replace developer = "Hudson Entertainment" 			if inlist(developer, "Hudson", "Hudson Entertainment", "Hudson Soft")
replace developer = "Hypnos Entertainment" 			if inlist(developer, "Hypnos", "Hypnos Entertainment")
replace developer = "Imageepoch" 					if inlist(developer, "Imageepoch", "Image Epoch")
replace developer = "Incognito Entertainment" 		if inlist(developer, "Incognito Inc.", "Incognito Entertainment")
replace developer = "indieszero" 					if inlist(developer, "indieszero", "Indies Zero")
replace developer = "Introversion Software" 		if inlist(developer, "Introversion", "Introversion Software")
replace developer = "InXile Entertainment" 			if inlist(developer, "inXile Entertainment", "InXile Entertainment")
replace developer = "IO Interactive" 				if inlist(developer, "Io Interactive", "IO Interactive")
replace developer = "IR Gurus Interactive Ltd." 	if inlist(developer, "IR Gurus Interactive Ltd.", "IR Gurus")
replace developer = "Jupiter" 						if inlist(developer, "Jupiter Corporation", "Jupiter Multimedia", "Jupiter")
replace developer = "JV Games" 						if inlist(developer, "JV Games", "JV Games Inc.")
replace developer = "K2 LLC" 						if inlist(developer, "K2", "K2 LLC")
replace developer = "Keen Software House" 			if inlist(developer, "Keen Games", "Keen Software House")
replace developer = "Koei Tecmo" 					if inlist(developer, "Koei", "Koei Canada", "Koei Tecmo", "Koei Tecmo Games")
replace developer = "Level-5" 						if inlist(developer, "Level 5", "Level-5")
replace developer = "Ludia Inc." 					if inlist(developer, "Ludia", "Ludia Inc.")
replace developer = "Majesco Entertainment" 		if inlist(developer, "Majesco", "Majesco Entertainment", "Majesco Games")
replace developer = "Marvelous Entertainment" 		if inlist(developer, "Marvelous", "Marvelous AQL", "Marvelous Entertainment", "Marvelous Inc.")
replace developer = "Media.Vision" 					if inlist(developer, "Media Vision", "Media.Vision")
replace developer = "MicroProse Software" 			if inlist(developer, "MicroProse Software", "MicroProse")
replace developer = "Microsoft Game Studios" 		if inlist(developer, "Microsoft Game Studios", "Microsoft Games Studio", "Microsoft Game Studios Japan")
replace developer = "Midway Games" 					if inlist(developer, "Midway", "Midway Games")
replace developer = "MileStone Inc." 				if inlist(developer, "Milestone", "MileStone Inc.", "Milestone S.r.l", "Milestone srl")
replace developer = "Mindscape Inc." 				if inlist(developer, "Mindscape", "Mindscape Inc.", "Mindscape USA")
replace developer = "Mitchell Corporation" 		if inlist(developer, "Mitchell", "Mitchell Corporation")
* CONTINUE



replace developer = "NAPS Team" 					if inlist(developer, "NAPS team", "Naps Team")
replace developer = "RED Entertainment" 			if inlist(developer, "RED Entertainment", "Red Entertainment")
replace developer = "Skip Ltd." 					if inlist(developer, "skip Ltd.", "Skip Ltd.")
replace developer = "StormRegion" 					if inlist(developer, "Stormregion", "StormRegion")
replace developer = "Supermassive Games" 			if inlist(developer, "SuperMassive Games", "Supermassive Games")
replace developer = "Syn Sophia" 					if inlist(developer, "syn Sophia", "Syn Sophia")
replace developer = "Thatgamecompany" 				if inlist(developer, "Thatgamecompany", "ThatGameCompany")
replace developer = "Tose" 							if inlist(developer, "TOSE", "Tose")
replace developer = "Tri-Ace" 						if inlist(developer, "tri-Ace", "Tri-Ace")
replace developer = "Tri-Crescendo" 				if inlist(developer, "tri-Crescendo", "Tri-Crescendo")