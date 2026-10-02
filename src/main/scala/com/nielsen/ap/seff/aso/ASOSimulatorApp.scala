package com.nielsen.ap.seff.aso

import scala.collection.mutable
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ASOSimulatorApp {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession for local execution with in-memory datasets
    val spark = SparkSession.builder()
      .appName("Connect ASO True Execution Pipeline")
      .master("local[*]")
      .getOrCreate()

    // Reduce logging verbosity for clean output
    spark.sparkContext.setLogLevel("ERROR")

    val simulator = new ConnectASOSimulator(spark)
    simulator.executeSimulation()
    
    spark.stop()
  }
}

class ConnectASOSimulator(val spark: SparkSession) {
  // Import implicits for seamless Sequence to DataFrame conversion
  import spark.implicits._

  // Global Pipeline Configurations
  val execId: String = "7001662"
  val analysisId: String = "11003982"
  
  // Delta Lake SOR simulating intermediate checkpoints between DAGs
  val deltaStore: mutable.Map[String, DataFrame] = mutable.Map()

  println("=" * 100)
  println(s"🧬 CONNECT ASO PIPELINE INITIALIZED WITH ACTIVE SPARK TRANSFORMATIONS")
  println("=" * 100)

  def printExecutiveSummary(taskName: String, desc: String): Unit = {
    println("\n" + "▪" * 80)
    println(s"📊 $taskName : $desc")
    println("▪" * 80)
  }

  def printDataset(name: String, df: DataFrame, isInput: Boolean = false): Unit = {
    val stage = if (isInput) "INPUT DATASET" else "TRANSFORMED OUTPUT DATASET"
    println(s"\n[📦 SPARK LOGS | $stage: $name]")
    df.show(truncate = false)
  }

  // ==========================================
  // STAGE 1: PRE-PUBLISHING / PREVIEW DAG
  // ==========================================
  
  def taskSaveUserRequest(): Unit = {
    printExecutiveSummary("SAVE_USER_REQUEST", "Hydrates execution specifications from JSON string payload")
    
    val specsDF = Seq((
      execId, analysisId, "US", "TOTAL HAIR CARE", 0.05, "Masked", "Walmart"
    )).toDF("exec_id", "analysis_id", "region", "category_scope", "sales_share_threshold", "char_delivery_mode", "excluded_retailer")
    
    deltaStore("Study_Specs") = specsDF
    printDataset("Study_Specs (Configuration Array)", specsDF)
  }

  def taskMarketDefinition(): Unit = {
    printExecutiveSummary("MARKET_DEFINITION", "Joins physical stores to execution boundaries via Geo indexing")
    
    val rawStores = Seq(
      (1, "Store Target NY", 1000),
      (2, "Store Walmart TX", 1000), 
      (3, "Store CVS CA", 4001)
    ).toDF("store_id", "store_name", "geo_id")
    
    val marketMap = Seq((1000, "Focus Geo"), (4001, "National Bench")).toDF("geo_id", "geo_name")
    
    printDataset("Raw Store Array", rawStores, isInput = true)
    
    val mappedStores = rawStores.join(marketMap, "geo_id")
    deltaStore("Mapped_Stores") = mappedStores
    printDataset("Mapped_Stores (Geo Linked)", mappedStores)
  }

  def taskCategoryDefinition(): Unit = {
    printExecutiveSummary("CATEGORY_DEFINITION", "Filters product universe based on payload inclusion logic")
    
    val rawProducts = Seq(
      ("3600523456789", "LOREAL ELVIVE", "HAIR CARE"),
      ("111222333", "PRIVATE LABEL SHAMPOO", "HAIR CARE"),
      ("999999999", "COLGATE TOOTHPASTE", "ORAL CARE")
    ).toDF("upc", "brand", "category")
    
    printDataset("Raw Product Master Catalog", rawProducts, isInput = true)
    
    val categoryScope = deltaStore("Study_Specs").select("category_scope").first().getString(0)
    val activeProducts = rawProducts.filter($"category".contains("HAIR"))
    
    deltaStore("Active_Products") = activeProducts
    printDataset("Active_Products (Filtered by Payload)", activeProducts)
  }

  // ==========================================
  // STAGE 2: DATA PREPARATION & CONNECT DAG (LEG 1)
  // ==========================================

  def taskAsoRmsDataPreparation(): Unit = {
    printExecutiveSummary("ASO_RMS_DATA_PREPARATION", "Cross joins active stores/products with base transactions & applies PI masking")
    
    val rawTxn = Seq(
      ("3600523456789", 1, 150.0, 15),
      ("111222333", 1, 45.0, 5),
      ("3600523456789", 2, 200.0, 20)
    ).toDF("upc", "store_id", "sales", "units")
    
    val activeProducts = deltaStore("Active_Products")
    val joinedTxn = rawTxn.join(activeProducts, "upc")
    
    printDataset("Base Micro Transactions (Joined with Products)", joinedTxn, isInput = true)
    
    val isMasked = deltaStore("Study_Specs").select("char_delivery_mode").first().getString(0) == "Masked"
    
    val preppedTxn = if(isMasked) {
      joinedTxn.withColumn("brand", when($"brand" === "PRIVATE LABEL SHAMPOO", lit("0000MASKED")).otherwise($"brand"))
    } else joinedTxn
    
    deltaStore("Prepped_Txn") = preppedTxn
    printDataset("Prepped_Txn (Masking Applied)", preppedTxn)
  }

  def taskAsoDataRestriction(): Unit = {
    printExecutiveSummary("ASO_DATA_RESTRICTION", "Releasability Rule Engine: Purges explicit restricted retail transactions")
    
    val excludedRetailer = deltaStore("Study_Specs").select("excluded_retailer").first().getString(0)
    val stores = deltaStore("Mapped_Stores")
    
    val txnWithStore = deltaStore("Prepped_Txn").join(stores, "store_id")
    
    printDataset("Input Transactions (Pre-Restriction)", txnWithStore, isInput = true)
    
    // Applying business logic restriction dynamically
    val restrictedTxn = txnWithStore.filter(!$"store_name".contains(excludedRetailer))
    
    deltaStore("Restricted_Txn") = restrictedTxn
    printDataset("Restricted_Txn (Walmart Purged)", restrictedTxn)
  }

  // ==========================================
  // STAGE 3: MATHEMATICAL ENGINE MODELING (LEG 1)
  // ==========================================

  def taskAsoModelBuildUniverse(): Unit = {
    printExecutiveSummary("ASO_MODEL_BUILD_UNIVERSE", "Expands transaction ledger with Covariate structural columns")
    
    val inputTxn = deltaStore("Restricted_Txn")
    printDataset("Input Restricted_Txn", inputTxn, isInput = true)
    
    val universe = inputTxn
      .withColumn("is_focus_geo", when($"geo_name" === "Focus Geo", 1).otherwise(0))
      .withColumn("is_target_retailer", when($"store_name".contains("Target"), 1).otherwise(0))
      
    deltaStore("Universe") = universe
    printDataset("Universe (Covariates Appended)", universe)
  }

  def taskAsoModelProductDefinitionLeg1(): Unit = {
    printExecutiveSummary("ASO_MODEL_PRODUCT_DEFINITION", "Engineers automatic structural parent-child hierarchy & applies dynamic pruning")
    
    val universe = deltaStore("Universe")
    printDataset("Input Universe Model Space", universe, isInput = true)
    
    val totalSales = universe.agg(sum("sales")).first().getDouble(0)
    val threshold = deltaStore("Study_Specs").select("sales_share_threshold").first().getDouble(0)
    
    val hierarchy = universe.groupBy("brand").agg(sum("sales").alias("brand_sales"))
      .withColumn("share_pct", round(($"brand_sales" / totalSales), 4))
      .withColumn("node_flag", when($"share_pct" < threshold, lit("AO LOW DIST")).otherwise(lit("Active Focus Node")))
      
    deltaStore("Hierarchy") = hierarchy
    printDataset("Hierarchy (Structural Groups Assessed)", hierarchy)
  }

  // ==========================================
  // STAGE 4: PIPELINE RESUMPTION & CONNECT DAG (LEG 2)
  // ==========================================

  def taskAsoRmsPreAggregation(): Unit = {
    printExecutiveSummary("ASO_RMS_PRE_AGGREGATION", "Collapses UPC level grain into Brand / Macro-Line store combinations")
    
    val universe = deltaStore("Universe")
    printDataset("Raw Item-Grain Store Sales", universe, isInput = true)
    
    val aggTxn = universe.groupBy("store_id", "brand")
      .agg(sum("sales").alias("total_sales"), sum("units").alias("total_units"))
      
    deltaStore("Agg_Txn") = aggTxn
    printDataset("Agg_Txn (Collapsed Grain)", aggTxn)
  }

  def taskAsoSlowMover(): Unit = {
    printExecutiveSummary("ASO_SLOW_MOVER", "Imputation for data sparsity stabilization across tracking arrays")
    
    val aggTxn = deltaStore("Agg_Txn")
    printDataset("Base Aggregated Fact Array", aggTxn, isInput = true)
    
    // Inject micro epsilons to stabilize downstream log denominators
    val slowMover = aggTxn.withColumn("total_sales", when($"total_sales" < 50, $"total_sales" + 0.00001).otherwise($"total_sales"))
    
    deltaStore("Slow_Mover") = slowMover
    printDataset("Slow_Mover (Imputed Zeroes)", slowMover)
  }

  def taskAsoRmsFactGeneration(): Unit = {
    printExecutiveSummary("ASO_RMS_FACT_GENERATION", "Derives historical velocity facts (Rate Of Sales)")
    
    val slowMover = deltaStore("Slow_Mover")
    printDataset("Imputed Sales Array", slowMover, isInput = true)
    
    val rmsFact = slowMover.withColumn("ROS_fact", round($"total_sales" / $"total_units", 2))
    
    deltaStore("Rms_Fact") = rmsFact
    printDataset("Rms_Fact (Computed Velocities)", rmsFact)
  }

  // ==========================================
  // STAGE 5: COMPUTATIONAL MATH ENGINE CORE (LEG 2)
  // ==========================================

  def taskGoAmanModelMixedModel(): Unit = {
    printExecutiveSummary("MIXED_MODEL_NAG", "Mixed Effects evaluation to compute base coefficient elasticity vectors (Direct Impact)")
    
    val rmsFact = deltaStore("Rms_Fact")
    printDataset("Historical Fact Matrix", rmsFact, isInput = true)
    
    // Formula simulation: DI is intrinsically tied to ROS factored by seasonal constraints
    val mixedModel = rmsFact.withColumn("direct_impact_raw", round($"ROS_fact" * 1.35, 2))
    
    deltaStore("Mixed_Model") = mixedModel
    printDataset("Mixed_Model (Elasticity Derived)", mixedModel)
  }

  def taskCoefficientAdjustments(): Unit = {
    printExecutiveSummary("COEFFICIENT_ADJUSTMENTS", "Operational Reliability Guardrails: Clamping Direct Impact metrics")
    
    val mixedModel = deltaStore("Mixed_Model")
    printDataset("Raw Mathematical Models", mixedModel, isInput = true)
    
    // Limit constraint: Direct Impact cannot logically exceed 150% of its ROS
    val adjustedModel = mixedModel.withColumn("di_clamped", 
      when($"direct_impact_raw" > ($"ROS_fact" * 1.5), $"ROS_fact" * 1.5).otherwise($"direct_impact_raw"))
      
    deltaStore("Adjusted_Model") = adjustedModel
    printDataset("Adjusted_Model (Statistically Clamped)", adjustedModel)
  }

  def taskQmatrixGeneration(): Unit = {
    printExecutiveSummary("ASO_MODEL_GENERATION (Q-MATRIX)", "Constructs relational mathematical substitution matrices (Loyalty / Switching)")
    
    val adjustedModel = deltaStore("Adjusted_Model")
    printDataset("Constrained Impact Matrix", adjustedModel, isInput = true)
    
    // Diagonal element evaluation
    val qMatrix = adjustedModel.withColumn("loyalty_index", 
      greatest(lit(0.005), lit(1.0) - round($"di_clamped" / $"ROS_fact", 4)))
      
    deltaStore("QMatrix") = qMatrix
    printDataset("QMatrix (Loyalty Substitution Ledger)", qMatrix)
  }

  // ==========================================
  // STAGE 6: PHASE 2 ASSORTMENT RECOMMENDATION (STORE GRAIN)
  // ==========================================

  def taskStoreRateOfSales(): Unit = {
    printExecutiveSummary("STORE_RATE_OF_SALES", "Granular physical velocity tracking by localized Store IDs")
    
    val qMatrix = deltaStore("QMatrix")
    printDataset("Macro Relational Matrix", qMatrix, isInput = true)
    
    val storeVelocity = qMatrix.groupBy("store_id").agg(avg("ROS_fact").alias("store_avg_velocity"))
    
    deltaStore("Store_Velocity") = storeVelocity
    printDataset("Store_Velocity (Physical Aggregation)", storeVelocity)
  }

  def taskStoreAssortmentRecommendation(): Unit = {
    printExecutiveSummary("STORE_ASSORTMENT_RECOMMENDATION", "Engineers final Keep/Remove decisions optimizing ROS boundaries")
    
    val qMatrix = deltaStore("QMatrix")
    printDataset("Input Substitution Framework", qMatrix, isInput = true)
    
    // Rank products partitioned by Store based on Loyalty & Velocity
    val windowSpec = Window.partitionBy("store_id").orderBy(desc("ROS_fact"), desc("loyalty_index"))
    
    val recommendations = qMatrix.withColumn("rank", rank().over(windowSpec))
      .withColumn("action_status", when($"rank" === 1, lit("KEEP")).otherwise(lit("REMOVE")))
      .withColumn("forecast_sales", when($"action_status" === "KEEP", $"total_sales" * 1.05).otherwise(0.0))
      .select("store_id", "brand", "action_status", "total_sales", "forecast_sales", "loyalty_index", "rank")
      
    // Applying native spark string formatting
    val finalOutput = recommendations
      .withColumn("total_sales", format_string("$%.2f", $"total_sales"))
      .withColumn("forecast_sales", format_string("$%.2f", $"forecast_sales"))
      
    printDataset("Final Store Assortment Recommendation Output", finalOutput)
  }

  def executeSimulation(): Unit = {
    taskSaveUserRequest()
    taskMarketDefinition()
    taskCategoryDefinition()
    
    // Leg 1 Data Prep
    taskAsoRmsDataPreparation()
    taskAsoDataRestriction()
    
    // Leg 1 Mathematical Core
    taskAsoModelBuildUniverse()
    taskAsoModelProductDefinitionLeg1()
    
    // Leg 2 Resume / Model Pre-Aggregation
    taskAsoRmsPreAggregation()
    taskAsoSlowMover()
    taskAsoRmsFactGeneration()
    
    // Leg 2 Math Subsystem
    taskGoAmanModelMixedModel()
    taskCoefficientAdjustments()
    taskQmatrixGeneration()
    
    // Phase 2 Recommender Space
    taskStoreRateOfSales()
    taskStoreAssortmentRecommendation()
    
    println("\n" + "="*100)
    println(s"🏁 PIPELINE RUN SUCCESSFUL: Spark DataFrames propagated transformations successfully.")
    println("="*100)
  }
}
