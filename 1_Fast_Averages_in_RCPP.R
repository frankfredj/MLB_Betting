################################################
################ INITIALISATION ################
################################################

options(scipen = 999)

#Libs
library("Rcpp")
library("RcppArmadillo")
library("RcppParallel")
library("stringr")
library("microbenchmark")

n = 25
path = "D:/MLB"
ncores <- 8

#Load cpp code
cpp_path <- "D:/1_MCMC.cpp"
sourceCpp(cpp_path)

library("sqldf")

################################################
################################################


############################################################
################ Build required data frames ################
############################################################


#Retrieve data
bat <- read.csv(paste(path, "/MLB_Modeling/Bat/Clean_Data/FanGraphs_Box_Scores.csv", sep = ""))
pitch <- read.csv(paste(path, "/MLB_Modeling/Pitch/Clean_Data/FanGraphs_Box_Scores.csv", sep = ""))
scores <- read.csv(paste(path, "/MLB_Modeling/Scores/Clean_Data/FanGraphs_Scores.csv", sep = ""))

#Compute certain missing metrics
pitch$BIP <- (pitch$H - pitch$HR) / pitch$BABIP
pitch$BIP[which(is.na(pitch$BIP) | is.infinite(pitch$BIP))] <- 0
pitch$BIP <- round(pitch$BIP)

pitch$LI <- pitch$WPA / pitch$WPA.LI
pitch$LI[which(is.na(pitch$LI) | is.infinite(pitch$LI))] <- 0

bat$LI <- bat$WPA / bat$WPA.LI
bat$LI[which(is.na(bat$LI) | is.infinite(bat$LI))] <- 0



bat$Date_Numeric <- floor(as.numeric(as.POSIXct(bat$Date, "%Y-%m-%d", tz = "")) / (24*60*60))
pitch$Date_Numeric <- floor(as.numeric(as.POSIXct(pitch$Date, "%Y-%m-%d", tz = "")) / (24*60*60))
scores$Date_Numeric <- floor(as.numeric(as.POSIXct(scores$Date, "%Y-%m-%d", tz = "")) / (24*60*60))

scores$Team_Home_numeric <- as.numeric(scores$Team_Home)
scores$Team_Away_numeric <- as.numeric(scores$Team_Away)


#Order data frames to allow sub-indexing via RcppArmadillo
bat <- sqldf("Select * from bat ORDER BY Name, Date_Numeric, Team DESC")
pitch <- sqldf("Select * from pitch ORDER BY Name, Date_Numeric, Team DESC")


#Keep linear metrics
col_index <- sort(c("BO", "PA", "H", "HR", "R", "RBI", "BB", "SO", "IBB", 
					"X1B", "X2B", "X3B", "HBP", "SF", "SH", "GDP", "SB", "CS",
						"GB", "FB", "LD", "IFFB", "IFH", "BU", "BUH", "Balls",
							"Strikes", "Pitches", "WPA", "WPA_plus", "WPA_minus", "PH",
								"pLI", "AB", "wRC.", "Spd", "wSB", "wRC", "RE24", "LI", "phLI"))

bat_names <- bat[c("Name", "Team", "ID", "Opponent", "Date", "Date_Numeric")]
bat_names$Name_Numeric <- as.numeric(bat_names$Name)
bat <- as.matrix(bat[col_index])

col_index = sort(c("IP", "TBF", "H", "HR", "ER", "BB", "SO", "pLI", "BS",
				"GS", "G", "CG", "ShO", "SV", "HLD", "R", "IBB", "IFH",
					"HBP", "WP", "BK", "WPA", "LD", "FB", "GB", "BUH", "tERA",
						"IFFB", "BU", "RS", "Balls", "Strikes", "Pitches",
							"WPA_plus", "WPA_minus", "RE24", "inLI", "gmLI", "exLI", "LI", "BIP"))

pitch_names <- pitch[c("Name", "ID", "Team", "Opponent", "Starting", "Date", "Date_Numeric")]
pitch_names$Name_Numeric <- as.numeric(pitch_names$Name)
pitch <- as.matrix(pitch[col_index])


#Build relief pitcher database
row_index <- which(pitch_names$Starting == "No")
pitch_relief_names <- pitch_names[row_index,]
pitch_relief <- pitch[row_index,]

pitch_relief_names$Name <- as.factor(paste("ReliefPitcher", pitch_relief_names$Team, sep = ""))
pitch_relief_names$Name_Numeric <- as.numeric(pitch_relief_names$Name)

temp1 <- unique(pitch_relief_names)
temp2 <- matrix(nrow = nrow(temp1), ncol = ncol(pitch_relief))

pb <- txtProgressBar(min = 0, max = nrow(temp2), style = 3)
for(i in 1:nrow(temp1)){

	index_row_id <- find_equal(pitch_relief_names$ID, temp1$ID[i])
	index_row <- index_row_id[find_equal(pitch_relief_names$Name_Numeric[index_row_id], temp1$Name_Numeric[i])]

	if(length(index_row) > 1){

		temp2[i,] <- column_sums_parallel(pitch_relief[index_row, ], 8)

	} else {

		temp2[i,] <- pitch_relief[index_row, ]

	}

	setTxtProgressBar(pb, i)

}

colnames(temp1) <- colnames(pitch_relief_names)
colnames(temp2) <- colnames(pitch_relief)

pitch_relief_names <- temp1
temp1 <- NULL
pitch_relief <- temp2
temp2 <- NULL

bat_names$Team_numeric <- as.numeric(bat_names$Team)
pitch_names$Team_numeric <- as.numeric(pitch_names$Team)
pitch_relief_names$Team_numeric <- as.numeric(pitch_relief_names$Team)


#Create starting pitcher database
pitch_starting_index <- which(pitch_names$Starting == "Yes")
pitch_starting <- pitch[pitch_starting_index,]
pitch_starting_names <- pitch_names[pitch_starting_index,]


#Save data
out <- list()
out$bat <- list()
out$bat$X <- bat
out$bat$names <- bat_names

out$pitch <- list()
out$pitch$X <- pitch
out$pitch$names <- pitch_names

out$pitch_relief <- list()
out$pitch_relief$X <- pitch_relief
out$pitch_relief$names <- pitch_relief_names

out$pitch_starting <- list()
out$pitch_starting$X <- pitch_starting
out$pitch_starting$names <- pitch_starting_names

out$scores <- scores

path_save <- paste(path, "/MLB_Modeling/Regression/R_frames.rds", sep = "")
saveRDS(out, path_save)

out <- NULL

################################################
################################################




################################################
################ Functions #####################
################################################

ncols <- (ncol(bat) + ncol(pitch) - 2)*2
weight_indices <- c(which(colnames(bat) == "PA"), which(colnames(pitch) == "IP"))



#Wrapper to extract all IDs
fit_all_IDs <- function(scores, 
						bat, bat_names, 
						pitch, pitch_names, 
						pitch_starting, pitch_starting_names,
						pitch_relief, pitch_relief_names,
						weight_indices,
						ndays,
						n_cols,
						n_cores){

	cnames_1 <- paste("Bat_", colnames(bat), "_Home", sep = "")[-weight_indices[1]]
	cnames_2 <- paste("Pitch_", colnames(pitch), "_Home", sep = "")[-weight_indices[2]]
	cnames_3 <- paste("Bat_", colnames(bat), "_Away", sep = "")[-weight_indices[1]]
	cnames_4 <- paste("Pitch_", colnames(pitch), "_Away", sep = "")[-weight_indices[2]]

	cnames <- c(cnames_1, cnames_2, cnames_3, cnames_4)	

	output <- query_ID_list(scores$ID, ndays, weight_indices[1], weight_indices[2], n_cols, n_cores,

											scores$ID,
						                    scores$Date_Numeric, 
						                    scores$Team_Home_numeric, 
						                    scores$Team_Away_numeric,

	                                        bat,
	                                        bat_names$ID,
	                                        bat_names$Name_Numeric,  
	                                        bat_names$Team_numeric,
	                                        bat_names$Date_Numeric,

	                                        pitch,
	                                        pitch_names$Name_Numeric,  
	                                        pitch_names$Date_Numeric,

	                                        pitch_starting,
	                                        pitch_starting_names$ID,
	                                        pitch_starting_names$Name_Numeric,  
	                                        pitch_starting_names$Team_numeric,
	                                        pitch_starting_names$Date_Numeric,                                        

	                                        pitch_relief,
	                                        pitch_relief_names$ID,
	                                        pitch_relief_names$Name_Numeric,  
	                                        pitch_relief_names$Team_numeric,
	                                        pitch_relief_names$Date_Numeric)

	colnames(output) <- cnames
	rownames(output) <- scores$ID

	keep <- which(apply(output, 1, sum) > 0)
	output <- output[keep, ]

	return(output)

}


################################################
################################################



#####################################################
################ Compute Frame  #####################
#####################################################



average_frame <-   fit_all_IDs(scores, 
								bat, bat_names, 
								pitch, pitch_names, 
								pitch_starting, pitch_starting_names,
								pitch_relief, pitch_relief_names,
								weight_indices,
								n,
								ncols,
								ncores)


#####################################################
#####################################################

path_save <- paste(path, "/MLB_Modeling/Regression/", n, "/R_regression_matrix.rds", sep = "")
saveRDS(average_frame, path_save)


#####################################################
################ Compute Frame  #####################
#####################################################


