import os
import sys
import time
import pandas as pd
from tabulate import tabulate

sys.stdout.reconfigure(encoding='utf-8')

class ConnectASOSimulator:
    def __init__(self):
        # Unpacking global system variables representing the UI payload
        self.exec_id = os.getenv("EXECUTION_ID", "7001662")
        self.analysis_id = os.getenv("ANALYSIS_ID", "11003982")
        self.version_id = os.getenv("VERSION_ID", "7000988")
        self.region = os.getenv("GEN2REGION", "US")
        self.env = os.getenv("GEN2ENVIRONMENT", "UAT")
        self.sim_mode = os.getenv("SIMULATION_MODE", "N").upper()
        self.initial_stage = os.getenv("STUDY_STAGE", "segmentation").lower()

        # Simulated Delta Lake System-Of-Record (SOR) Storage Registers
        self.delta_store = {}
        
        print("=" * 100)
        print(f"🧬 CONNECT ASO PIPELINE INITIALIZED WITH ACTIVE PANDAS TRANSFORMATIONS")
        print("=" * 100)

    def print_executive_summary(self, task_name, layer, scope, desc):
        print("\n" + "▪" * 80)
        print(f"📊 EXECUTIVE SUMMARY: {task_name}")
        print(f"  • Architecture Layer : {layer}")
        print(f"  • Operational Scope  : {scope}")
        print(f"  • Business Objective : {desc}")
        print("▪" * 80)

    def print_dataset(self, name, df, is_input=False):
        stage = "INPUT DATASET" if is_input else "TRANSFORMED OUTPUT DATASET"
        print(f"\n[📦 PANDAS LOGS | {stage}: {name}]")
        print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    # ==========================================
    # STAGE 1: PRE-PUBLISHING / PREVIEW DAG
    # ==========================================
    
    def task_save_user_request(self):
        self.print_executive_summary(
            "SAVE_USER_REQUEST", 
            "Airflow Orchestrator ──► Pandas Core Component",
            "Metadata Hydration Layer",
            "Extracts raw configurations from UI payload string and builds the core Delta System of Record."
        )
        specs_data = [{
            "exec_id": self.exec_id, "analysis_id": self.analysis_id, "region": self.region,
            "category_scope": "TOTAL HAIR CARE", "sales_share_threshold": 0.05, 
            "char_delivery_mode": "Masked", "excluded_retailer": "Walmart"
        }]
        specs_df = pd.DataFrame(specs_data)
        self.delta_store["Study_Specs"] = specs_df
        self.print_dataset("Study_Specs (Configuration Array)", specs_df)

    def task_market_definition(self):
        self.print_executive_summary(
            "MARKET_DEFINITION",
            "Pandas API over Database Boundary",
            "Geographical Boundary Structuring",
            "Parses regional store lists to allocate custom Execution Areas (EA)."
        )
        raw_stores = pd.DataFrame([
            (1, "Store Target NY", 1000),
            (2, "Store Walmart TX", 1000),
            (3, "Store CVS CA", 4001)
        ], columns=["store_id", "store_name", "geo_id"])
        
        market_map = pd.DataFrame([
            (1000, "Focus Geo"), 
            (4001, "National Bench")
        ], columns=["geo_id", "geo_name"])
        
        self.print_dataset("Raw Store Array", raw_stores, is_input=True)
        
        mapped_stores = pd.merge(raw_stores, market_map, on="geo_id", how="inner")
        self.delta_store["Mapped_Stores"] = mapped_stores
        self.print_dataset("Mapped_Stores (Geo Linked)", mapped_stores)

    def task_category_definition(self):
        self.print_executive_summary(
            "CATEGORY_DEFINITION",
            "Pandas Engine Interface Core",
            "Product Inventory Scoping",
            "Filters upstream product universes down to target evaluation boundaries matching rulesets."
        )
        raw_products = pd.DataFrame([
            ("3600523456789", "LOREAL ELVIVE", "HAIR CARE"),
            ("111222333", "PRIVATE LABEL SHAMPOO", "HAIR CARE"),
            ("999999999", "COLGATE TOOTHPASTE", "ORAL CARE")
        ], columns=["upc", "brand", "category"])
        
        self.print_dataset("Raw Product Master Catalog", raw_products, is_input=True)
        
        active_products = raw_products[raw_products["category"].str.contains("HAIR")]
        self.delta_store["Active_Products"] = active_products
        self.print_dataset("Active_Products (Filtered by Payload)", active_products)

    # ==========================================
    # STAGE 2: DATA PREPARATION & CONNECT DAG (LEG 1)
    # ==========================================

    def task_aso_rms_data_preparation(self):
        self.print_executive_summary(
            "ASO_RMS_DATA_PREPARATION",
            "Pandas Execution Class Node",
            "Micro Transaction Consolidation",
            "Cross joins active stores/products with base transactions & applies PI masking."
        )
        raw_txn = pd.DataFrame([
            ("3600523456789", 1, 150.0, 15),
            ("111222333", 1, 45.0, 5),
            ("3600523456789", 2, 200.0, 20)
        ], columns=["upc", "store_id", "sales", "units"])
        
        active_products = self.delta_store["Active_Products"]
        joined_txn = pd.merge(raw_txn, active_products, on="upc", how="inner")
        self.print_dataset("Base Micro Transactions (Joined with Products)", joined_txn, is_input=True)
        
        is_masked = self.delta_store["Study_Specs"].iloc[0]["char_delivery_mode"] == "Masked"
        
        prepped_txn = joined_txn.copy()
        if is_masked:
            mask_cond = prepped_txn["brand"] == "PRIVATE LABEL SHAMPOO"
            prepped_txn.loc[mask_cond, "brand"] = "0000MASKED"
            
        self.delta_store["Prepped_Txn"] = prepped_txn
        self.print_dataset("Prepped_Txn (Masking Applied)", prepped_txn)

    def task_aso_data_restriction(self):
        self.print_executive_summary(
            "ASO_DATA_RESTRICTION",
            "Rule Engine Validation Network",
            "Compliance & Governance Guardrails",
            "Applies regional distribution exclusions and policy-driven compliance rules."
        )
        excluded_retailer = self.delta_store["Study_Specs"].iloc[0]["excluded_retailer"]
        txn_with_store = pd.merge(self.delta_store["Prepped_Txn"], self.delta_store["Mapped_Stores"], on="store_id", how="inner")
        
        self.print_dataset("Input Transactions (Pre-Restriction)", txn_with_store, is_input=True)
        
        restricted_txn = txn_with_store[~txn_with_store["store_name"].str.contains(excluded_retailer)].copy()
        self.delta_store["Restricted_Txn"] = restricted_txn
        self.print_dataset("Restricted_Txn (Walmart Purged)", restricted_txn)

    # ==========================================
    # STAGE 3: MATHEMATICAL ENGINE MODELING (LEG 1)
    # ==========================================

    def task_aso_model_build_universe(self):
        self.print_executive_summary(
            "ASO_MODEL_BUILD_UNIVERSE",
            "Pandas Computational Core Module",
            "Statistical Covariate Generation",
            "Maps store-level characteristics and geography models to prepare regression matrices."
        )
        input_txn = self.delta_store["Restricted_Txn"].copy()
        self.print_dataset("Input Restricted_Txn", input_txn, is_input=True)
        
        input_txn["is_focus_geo"] = (input_txn["geo_name"] == "Focus Geo").astype(int)
        input_txn["is_target_retailer"] = input_txn["store_name"].str.contains("Target").astype(int)
        
        self.delta_store["Universe"] = input_txn
        self.print_dataset("Universe (Covariates Appended)", input_txn)

    def task_aso_model_product_definition_leg1(self):
        self.print_executive_summary(
            "ASO_MODEL_PRODUCT_DEFINITION (LEG 1)",
            "Pandas Distributed Architecture",
            "Hierarchy Compilation & Pruning",
            "Builds the automatic assortment tree structure while pruning small nodes."
        )
        universe = self.delta_store["Universe"]
        self.print_dataset("Input Universe Model Space", universe, is_input=True)
        
        total_sales = universe["sales"].sum()
        threshold = self.delta_store["Study_Specs"].iloc[0]["sales_share_threshold"]
        
        hierarchy = universe.groupby("brand")["sales"].sum().reset_index().rename(columns={"sales": "brand_sales"})
        hierarchy["share_pct"] = round(hierarchy["brand_sales"] / total_sales, 4)
        hierarchy["node_flag"] = hierarchy["share_pct"].apply(lambda x: "AO LOW DIST" if x < threshold else "Active Focus Node")
        
        self.delta_store["Hierarchy"] = hierarchy
        self.print_dataset("Hierarchy (Structural Groups Assessed)", hierarchy)

    # ==========================================
    # STAGE 4: PIPELINE RESUMPTION & CONNECT DAG (LEG 2)
    # ==========================================

    def task_aso_rms_pre_aggregation(self):
        self.print_executive_summary(
            "ASO_RMS_PRE_AGGREGATION",
            "Data Transformation Engine",
            "Item-Grain Product Mapping Consolidation",
            "Rolls high-grain transactions up to mapped model group identifiers."
        )
        universe = self.delta_store["Universe"]
        self.print_dataset("Raw Item-Grain Store Sales", universe, is_input=True)
        
        agg_txn = universe.groupby(["store_id", "brand"]).agg(
            total_sales=("sales", "sum"),
            total_units=("units", "sum")
        ).reset_index()
        
        self.delta_store["Agg_Txn"] = agg_txn
        self.print_dataset("Agg_Txn (Collapsed Grain)", agg_txn)

    def task_aso_slow_mover(self):
        self.print_executive_summary(
            "ASO_SLOW_MOVER",
            "Pandas Analytical Component",
            "Data Sparsity Imputation Modeling",
            "Identifies thin-distribution products and injects low-volume data parameters."
        )
        agg_txn = self.delta_store["Agg_Txn"].copy()
        self.print_dataset("Base Aggregated Fact Array", agg_txn, is_input=True)
        
        agg_txn["total_sales"] = agg_txn["total_sales"].apply(lambda x: x + 0.00001 if x < 50 else x)
        self.delta_store["Slow_Mover"] = agg_txn
        self.print_dataset("Slow_Mover (Imputed Zeroes)", agg_txn)

    def task_aso_rms_fact_generation(self):
        self.print_executive_summary(
            "ASO_RMS_FACT_GENERATION",
            "Matrix Compilation Engine",
            "Rolling Matrix Fact Generation",
            "Computes baseline volumetric velocities and aggregates historical data."
        )
        slow_mover = self.delta_store["Slow_Mover"].copy()
        self.print_dataset("Imputed Sales Array", slow_mover, is_input=True)
        
        slow_mover["ROS_fact"] = round(slow_mover["total_sales"] / slow_mover["total_units"], 2)
        self.delta_store["Rms_Fact"] = slow_mover
        self.print_dataset("Rms_Fact (Computed Velocities)", slow_mover)

    # ==========================================
    # STAGE 5: COMPUTATIONAL MATH ENGINE CORE (LEG 2)
    # ==========================================

    def task_go_aman_model_mixed_model(self):
        self.print_executive_summary(
            "goAmanModel_mixedModel_NAG_by_nodeId_parent",
            "Pandas Workspace Pipeline",
            "Mixed-Effects Statistical Modeling Engine",
            "Fits distributed mixed-effects regressions across tree components."
        )
        rms_fact = self.delta_store["Rms_Fact"].copy()
        self.print_dataset("Historical Fact Matrix", rms_fact, is_input=True)
        
        rms_fact["direct_impact_raw"] = round(rms_fact["ROS_fact"] * 1.35, 2)
        self.delta_store["Mixed_Model"] = rms_fact
        self.print_dataset("Mixed_Model (Elasticity Derived)", rms_fact)

    def task_coefficient_adjustments(self):
        self.print_executive_summary(
            "COEFFICIENT_ADJUSTMENTS_STAGE_1_TO_3",
            "Pandas Pipeline Utility Library",
            "Algorithmic Bound Boundary Clamping",
            "Applies operational constraints to statistical outputs to ensure reliability."
        )
        mixed_model = self.delta_store["Mixed_Model"].copy()
        self.print_dataset("Raw Mathematical Models", mixed_model, is_input=True)
        
        mixed_model["di_clamped"] = mixed_model.apply(
            lambda row: row["ROS_fact"] * 1.5 if row["direct_impact_raw"] > (row["ROS_fact"] * 1.5) else row["direct_impact_raw"], 
            axis=1
        )
        self.delta_store["Adjusted_Model"] = mixed_model
        self.print_dataset("Adjusted_Model (Statistically Clamped)", mixed_model)

    def task_qmatrix_generation(self):
        self.print_executive_summary(
            "ASO_MODEL_GENERATION (Q-Matrix Constraint Pipeline)",
            "Pandas Matrix Processing Engine",
            "Cannibalization Substitution Interrelation Modeling",
            "Derives the final product substitution matrix using structural ceiling rules."
        )
        adj_model = self.delta_store["Adjusted_Model"].copy()
        self.print_dataset("Constrained Impact Matrix", adj_model, is_input=True)
        
        adj_model["loyalty_index"] = adj_model.apply(
            lambda row: max(0.005, 1.0 - round(row["di_clamped"] / row["ROS_fact"], 4)), 
            axis=1
        )
        self.delta_store["QMatrix"] = adj_model
        self.print_dataset("QMatrix (Loyalty Substitution Ledger)", adj_model)

    # ==========================================
    # STAGE 6: PHASE 2 ASSORTMENT RECOMMENDATION (STORE GRAIN)
    # ==========================================

    def task_store_rate_of_sales(self):
        self.print_executive_summary(
            "STORE_RATE_OF_SALES (PHASE 2)",
            "Pandas Calculation Core",
            "Localized Store Grain Velocity Analysis",
            "Shifts calculation limits to explicitly compute localized sales velocity per store asset."
        )
        qmatrix = self.delta_store["QMatrix"]
        self.print_dataset("Macro Relational Matrix", qmatrix, is_input=True)
        
        store_velocity = qmatrix.groupby("store_id")["ROS_fact"].mean().reset_index().rename(columns={"ROS_fact": "store_avg_velocity"})
        self.delta_store["Store_Velocity"] = store_velocity
        self.print_dataset("Store_Velocity (Physical Aggregation)", store_velocity)

    def task_store_assortment_recommendation(self):
        self.print_executive_summary(
            "STORE_ASSORTMENT_RECOMMENDATION (PHASE 2 OPTIMIZER)",
            "Pandas Optimization Algorithms",
            "Localized Store Assortment Optimization",
            "Swaps lower-ranked products for high-demand items to optimize shelf assortment."
        )
        qmatrix = self.delta_store["QMatrix"].copy()
        self.print_dataset("Input Substitution Framework", qmatrix, is_input=True)
        
        # Rank by ROS_fact desc, loyalty_index desc per store
        qmatrix.sort_values(by=["store_id", "ROS_fact", "loyalty_index"], ascending=[True, False, False], inplace=True)
        qmatrix["rank"] = qmatrix.groupby("store_id").cumcount() + 1
        
        qmatrix["action_status"] = qmatrix["rank"].apply(lambda x: "KEEP" if x == 1 else "REMOVE")
        qmatrix["forecast_sales"] = qmatrix.apply(lambda row: row["total_sales"] * 1.05 if row["action_status"] == "KEEP" else 0.0, axis=1)
        
        final_output = qmatrix[["store_id", "brand", "action_status", "total_sales", "forecast_sales", "loyalty_index", "rank"]].copy()
        final_output["total_sales"] = final_output["total_sales"].apply(lambda x: f"${x:.2f}")
        final_output["forecast_sales"] = final_output["forecast_sales"].apply(lambda x: f"${x:.2f}")
        
        self.print_dataset("Final Store Assortment Recommendation Output", final_output)

    def execute_simulation(self):
        # Stage 1: Preview Space
        self.task_save_user_request()
        self.task_market_definition()
        self.task_category_definition()
        
        # Branch Evaluation Check
        if self.sim_mode == "Y":
            print("\n⚠️ [Branch Warning] simulation_mode is enabled ('Y'). Bypassing calculation grid tasks.")
            return
            
        # Stage 2 & 3: Leg 1 Data Prep and Tree Construction
        self.task_aso_rms_data_preparation()
        self.task_aso_data_restriction()
        self.task_aso_model_build_universe()
        self.task_aso_model_product_definition_leg1()
        
        # Stage 4, 5 & 6: Leg 2 Processing, Advanced Math Core, and Store Optimization
        self.task_aso_rms_pre_aggregation()
        self.task_aso_slow_mover()
        self.task_aso_rms_fact_generation()
        self.task_go_aman_model_mixed_model()
        self.task_coefficient_adjustments()
        self.task_qmatrix_generation()
        self.task_store_rate_of_sales()
        self.task_store_assortment_recommendation()
        
        print("\n" + "="*100)
        print(f"🏁 PIPELINE RUN SUCCESSFUL: Pandas DataFrames propagated transformations successfully.")
        print("="*100)

if __name__ == "__main__":
    simulator = ConnectASOSimulator()
    simulator.execute_simulation()
