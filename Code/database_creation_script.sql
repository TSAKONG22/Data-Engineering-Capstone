CREATE DATABASE  IF NOT EXISTS `creditcard_capstone` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `creditcard_capstone`;
-- MySQL dump 10.13  Distrib 8.0.34, for Win64 (x86_64)
--
-- Host: localhost    Database: creditcard_capstone
-- ------------------------------------------------------
-- Server version	8.0.23

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `cdw_sapp_branch`
--

DROP TABLE IF EXISTS `cdw_sapp_branch`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `cdw_sapp_branch` (
  `BRANCH_CODE` int DEFAULT NULL,
  `BRANCH_NAME` longtext,
  `BRANCH_STREET` longtext,
  `BRANCH_CITY` longtext,
  `BRANCH_STATE` longtext,
  `BRANCH_ZIP` int NOT NULL,
  `BRANCH_PHONE` longtext NOT NULL,
  `LAST_UPDATED` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cdw_sapp_credit_card`
--

DROP TABLE IF EXISTS `cdw_sapp_credit_card`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `cdw_sapp_credit_card` (
  `CUST_CC_NO` longtext,
  `TIMEID` longtext NOT NULL,
  `CUST_SSN` int DEFAULT NULL,
  `BRANCH_CODE` int DEFAULT NULL,
  `TRANSACTION_TYPE` longtext,
  `TRANSACTION_VALUE` double DEFAULT NULL,
  `TRANSACTION_ID` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cdw_sapp_customer`
--

DROP TABLE IF EXISTS `cdw_sapp_customer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `cdw_sapp_customer` (
  `SSN` int DEFAULT NULL,
  `FIRST_NAME` longtext,
  `MIDDLE_NAME` longtext,
  `LAST_NAME` longtext,
  `Credit_card_no` longtext,
  `FULL_STREET_ADDRESS` longtext NOT NULL,
  `CUST_CITY` longtext,
  `CUST_STATE` longtext,
  `CUST_COUNTRY` longtext,
  `CUST_ZIP` int DEFAULT NULL,
  `CUST_PHONE` longtext NOT NULL,
  `CUST_EMAIL` longtext,
  `LAST_UPDATED` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cdw_sapp_loan_application`
--

DROP TABLE IF EXISTS `cdw_sapp_loan_application`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `cdw_sapp_loan_application` (
  `Application_ID` longtext,
  `Application_Status` longtext,
  `Credit_History` bigint DEFAULT NULL,
  `Dependents` longtext,
  `Education` longtext,
  `Gender` longtext,
  `Income` longtext,
  `Married` longtext,
  `Property_Area` longtext,
  `Self_Employed` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-10-19  7:34:19
