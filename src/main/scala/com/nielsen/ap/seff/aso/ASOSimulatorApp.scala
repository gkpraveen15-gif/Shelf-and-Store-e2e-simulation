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

  // ==========================================
  // STAGE 1: PRE-PUBLISHING / PREVIEW DAG
  // ==========================================
  
  def taskSaveUserRequest(): Unit = {
    printExecutiveSummary("SAVE_USER_REQUEST", "Hydrates execution specifications from JSON string payload")
    
    val specsDF = Seq((
      execId, analysisId, "US", "TOTAL HAIR CARE", 0.05, "Masked", "Walmart"
    )).toDF("exec_id", "analysis_id", "region", "category_scope", "sales_share_threshold", "char_delivery_mode", "excluded_retailer")
    
    deltaStore("Study_Specs") = specsDF
    specsDF.show(truncate = false)
  }

  def taskMarketDefinition(): Unit = {
    printExecutiveSummary("MARKET_DEFINITION", "Joins physical stores to execution boundaries via Geo indexing")
    
    val rawStores = Seq(
      (1, "Store Target NY", 1000),
      (2, "Store Walmart TX", 1000), 
      (3, "Store CVS CA", 4001)
    ).toDF("store_id", "store_name", "geo_id")
    
    val marketMap = Seq((1000, "Focus Geo"), (4001, "National Bench")).toDF("geo_id", "geo_name")
    
    val mappedStores = rawStores.join(marketMap, "geo_id")
    deltaStore("Mapped_Stores") = mappedStores
    mappedStores.show(truncate = false)
  }

  def taskCategoryDefinition(): Unit = {
    printExecutiveSummary("CATEGORY_DEFINITION", "Filters product universe based on payload inclusion logic")
    
    val rawProducts = Seq(
      ("3600523456789", "LOREAL ELVIVE", "HAIR CARE"),
      ("111222333", "PRIVATE LABEL SHAMPOO", "HAIR CARE"),
      ("999999999", "COLGATE TOOTHPASTE", "ORAL CARE")
    ).toDF("upc", "brand", "category")
    
    val categoryScope = deltaStore("Study_Specs").select("category_scope").first().getString(0)
    val activeProducts = rawProducts.filter($"category".contains("HAIR"))
    
    deltaStore("Active_Products") = activeProducts
    activeProducts.show(truncate = false)
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
    
    val isMasked = deltaStore("Study_Specs").select("char_delivery_mode").first().getString(0) == "Masked"
    
    val preppedTxn = if(isMasked) {
      joinedTxn.withColumn("brand", when($"brand" === "PRIVATE LABEL SHAMPOO", lit("0000MASKED")).otherwise($"brand"))
    } else joinedTxn
    
    deltaStore("Prepped_Txn") = preppedTxn
    preppedTxn.show(truncate = false)
  }

  def taskAsoDataRestriction(): Unit = {
    printExecutiveSummary("ASO_DATA_RESTRICTION", "Releasability Rule Engine: Purges explicit restricted retail transactions")
    
    val excludedRetailer = deltaStore("Study_Specs").select("excluded_retailer").first().getString(0)
    val stores = deltaStore("Mapped_Stores")
    
    val txnWithStore = deltaStore("Prepped_Txn").join(stores, "store_id")
    
    // Applying business logic restriction dynamically
    val restrictedTxn = txnWithStore.filter(!$"store_name".contains(excludedRetailer))
    
    deltaStore("Restricted_Txn") = restrictedTxn
    restrictedTxn.show(truncate = false)
  }

  // ==========================================
  // STAGE 3: MATHEMATICAL ENGINE MODELING (LEG 1)
  // ==========================================

  def taskAsoModelBuildUniverse(): Unit = {
    printExecutiveSummary("ASO_MODEL_BUILD_UNIVERSE", "Expands transaction ledger with Covariate structural columns")
    
    val universe = deltaStore("Restricted_Txn")
      .withColumn("is_focus_geo", when($"geo_name" === "Focus Geo", 1).otherwise(0))
      .withColumn("is_target_retailer", when($"store_name".contains("Target"), 1).otherwise(0))
      
    deltaStore("Universe") = universe
    universe.show(truncate = false)
  }

  def taskAsoModelProductDefinitionLeg1(): Unit = {
    printExecutiveSummary("ASO_MODEL_PRODUCT_DEFINITION", "Engineers automatic structural parent-child hierarchy & applies dynamic pruning")
    
    val universe = deltaStore("Universe")
    val totalSales = universe.agg(sum("sales")).first().getDouble(0)
    val threshold = deltaStore("Study_Specs").select("sales_share_threshold").first().getDouble(0)
    
    val hierarchy = universe.groupBy("brand").agg(sum("sales").alias("brand_sales"))
      .withColumn("share_pct", round(($"brand_sales" / totalSales), 4))
      .withColumn("node_flag", when($"share_pct" < threshold, lit("AO LOW DIST")).otherwise(lit("Active Focus Node")))
      
    deltaStore("Hierarchy") = hierarchy
    hierarchy.show(truncate = false)
  }

  // ==========================================
  // STAGE 4: PIPELINE RESUMPTION & CONNECT DAG (LEG 2)
  // ==========================================

  def taskAsoRmsPreAggregation(): Unit = {
    printExecutiveSummary("ASO_RMS_PRE_AGGREGATION", "Collapses UPC level grain into Brand / Macro-Line store combinations")
    
    val universe = deltaStore("Universe")
    val aggTxn = universe.groupBy("store_id", "brand")
      .agg(sum("sales").alias("total_sales"), sum("units").alias("total_units"))
      
    deltaStore("Agg_Txn") = aggTxn
    aggTxn.show(truncate = false)
  }

  def taskAsoSlowMover(): Unit = {
    printExecutiveSummary("ASO_SLOW_MOVER", "Imputation for data sparsity stabilization across tracking arrays")
    
    val aggTxn = deltaStore("Agg_Txn")
    // Inject micro epsilons to stabilize downstream log denominators
    val slowMover = aggTxn.withColumn("total_sales", when($"total_sales" < 50, $"total_sales" + 0.00001).otherwise($"total_sales"))
    
    deltaStore("Slow_Mover") = slowMover
    slowMover.show(truncate = false)
  }

  def taskAsoRmsFactGeneration(): Unit = {
    printExecutiveSummary("ASO_RMS_FACT_GENERATION", "Derives historical velocity facts (Rate Of Sales)")
    
    val slowMover = deltaStore("Slow_Mover")
    val rmsFact = slowMover.withColumn("ROS_fact", round($"total_sales" / $"total_units", 2))
    
    deltaStore("Rms_Fact") = rmsFact
    rmsFact.show(truncate = false)
  }

  // ==========================================
  // STAGE 5: COMPUTATIONAL MATH ENGINE CORE (LEG 2)
  // ==========================================

  def taskGoAmanModelMixedModel(): Unit = {
    printExecutiveSummary("MIXED_MODEL_NAG", "Mixed Effects evaluation to compute base coefficient elasticity vectors (Direct Impact)")
    
    val rmsFact = deltaStore("Rms_Fact")
    // Formula simulation: DI is intrinsically tied to ROS factored by seasonal constraints
    val mixedModel = rmsFact.withColumn("direct_impact_raw", round($"ROS_fact" * 1.35, 2))
    
    deltaStore("Mixed_Model") = mixedModel
    mixedModel.show(truncate = false)
  }

  def taskCoefficientAdjustments(): Unit = {
    printExecutiveSummary("COEFFICIENT_ADJUSTMENTS", "Operational Reliability Guardrails: Clamping Direct Impact metrics")
    
    val mixedModel = deltaStore("Mixed_Model")
    // Limit constraint: Direct Impact cannot logically exceed 150% of its ROS
    val adjustedModel = mixedModel.withColumn("di_clamped", 
      when($"direct_impact_raw" > ($"ROS_fact" * 1.5), $"ROS_fact" * 1.5).otherwise($"direct_impact_raw"))
      
    deltaStore("Adjusted_Model") = adjustedModel
    adjustedModel.show(truncate = false)
  }

  def taskQmatrixGeneration(): Unit = {
    printExecutiveSummary("ASO_MODEL_GENERATION (Q-MATRIX)", "Constructs relational mathematical substitution matrices (Loyalty / Switching)")
    
    val adjustedModel = deltaStore("Adjusted_Model")
    // Diagonal element evaluation
    val qMatrix = adjustedModel.withColumn("loyalty_index", 
      greatest(lit(0.005), lit(1.0) - round($"di_clamped" / $"ROS_fact", 4)))
      
    deltaStore("QMatrix") = qMatrix
    qMatrix.show(truncate = false)
  }

  // ==========================================
  // STAGE 6: PHASE 2 ASSORTMENT RECOMMENDATION (STORE GRAIN)
  // ==========================================

  def taskStoreRateOfSales(): Unit = {
    printExecutiveSummary("STORE_RATE_OF_SALES", "Granular physical velocity tracking by localized Store IDs")
    
    val qMatrix = deltaStore("QMatrix")
    val storeVelocity = qMatrix.groupBy("store_id").agg(avg("ROS_fact").alias("store_avg_velocity"))
    
    deltaStore("Store_Velocity") = storeVelocity
    storeVelocity.show(truncate = false)
  }

  def taskStoreAssortmentRecommendation(): Unit = {
    printExecutiveSummary("STORE_ASSORTMENT_RECOMMENDATION", "Engineers final Keep/Remove decisions optimizing ROS boundaries")
    
    val qMatrix = deltaStore("QMatrix")
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
      
    finalOutput.show(truncate = false)
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
