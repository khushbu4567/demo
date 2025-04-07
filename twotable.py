import mysql.connector
from groq import Groq
import streamlit as st
import pandas as pd
import re


# Database credentials
host = "localhost"
user = "root"
password = ""  # Keep empty if no password is set
database = "sd_factus"  # Change to your database name


# Initialize Groq client
client = Groq(api_key="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")  # Replace with your Groq API key


def extract_schema_info():
   """
   Extracts detailed schema information (tables/views and columns with data types) from the database.
   Returns comprehensive schema info for all views.
   """
   try:
       # Establishing connection
       conn = mysql.connector.connect(
           host=host,
           user=user,
           password=password,
           database=database
       )


       cursor = conn.cursor()


       # Fetch schema information for all views
       views = ["vw_product_detail", "vw_manufacture_report", "vw_ndc_detail_report", "fct_product_marketed_by"]
       schema_info = {}


       for view in views:
           # Get column names and types
           cursor.execute(f"DESCRIBE {view}")
           columns = cursor.fetchall()
           schema_info[view] = {col[0]: col[1] for col in columns}  # Store column names and types
          
           # Get sample data to understand content (limited to 1 row)
           try:
               cursor.execute(f"SELECT * FROM {view} LIMIT 1")
               sample = cursor.fetchone()
               schema_info[f"{view}_sample"] = sample
           except:
               schema_info[f"{view}_sample"] = None


       # Close the connection
       cursor.close()
       conn.close()


       return schema_info


   except mysql.connector.Error as e:
       st.error(f"Error connecting to MySQL: {e}")
       return None


def determine_best_view(user_question, schema_info):
   """
   Uses LLM to determine which view is most likely to answer the user's question.
   Returns the view name that should be tried first.
   """
   # Prepare view descriptions based on column names
   view_descriptions = {}
   for view in schema_info:
       if not view.endswith("_sample"):
           columns = list(schema_info[view].keys())
           view_descriptions[view] = f"View {view} contains columns: {', '.join(columns)}"
  
   # Prepare the LLM prompt
   view_info = "\n".join([f"{view}: {desc}" for view, desc in view_descriptions.items()])
  
   prompt = f"""Given the following views in a pharmaceutical database:


{view_info}


And given the user question: "{user_question}"


Which view would be most appropriate to query first to answer this question? Consider the subject matter and specific column needs.
For questions about marketing or marketed products, always choose fct_product_marketed_by first.
Only respond with the exact view name (one of: vw_product_detail, vw_manufacture_report, vw_ndc_detail_report, or fct_product_marketed_by).
Do not include any explanation or additional text.
"""


   # Call the Groq API for view selection
   try:
       response = client.chat.completions.create(
           model="llama-3.1-8b-instant",
           messages=[
               {"role": "system", "content": "You are a pharmaceutical database SQL expert who generates precise, correct SQL queries that follow all rules. You carefully analyze questions and select only the specific columns needed rather than using SELECT *."},
               {"role": "user", "content": prompt}
           ],
           max_tokens=100,
           temperature=0.1,
       )
      
       view_name = response.choices[0].message.content.strip().lower()
      
       # Validate the returned view name
       valid_views = ["vw_product_detail", "vw_manufacture_report", "vw_ndc_detail_report", "fct_product_marketed_by"]
       
       # Check if the question is about marketing - if so, prioritize fct_product_marketed_by
       question_lower = user_question.lower()
       if ("market" in question_lower or "marketed" in question_lower) and "fct_product_marketed_by" in valid_views:
           return "fct_product_marketed_by"
       
       if view_name in valid_views:
           return view_name
       else:
           # Default to the most comprehensive view if LLM returns invalid response
           return "vw_product_detail"
          
   except Exception as e:
       st.error(f"Error determining best view: {e}")
       # Default view if LLM fails
       return "vw_product_detail"


def generate_sql_query_for_view(user_question, schema_info, target_view):
   """
   Generates an SQL query for a specific view using the Groq API.
   Enhanced to provide detailed column information and improve query accuracy.
   Now extracts only specific columns requested in the question.
   """
   # Get column info for the target view
   view_columns = schema_info[target_view]
   column_list = ", ".join(view_columns.keys())
  
   # Add information about all important columns
   column_descriptions = """
   CRITICAL COLUMN INFORMATION:
   - ApplictionNo: Application number used by applicant to file (brand reference application number)
   - ActiveIngredient: Main ingredient of the drug
   - BrandName: Brand or reference drug name
   - Form: Form in which drug is available
   - Dosage: Power/strength/dosage of the drug
   - ApplicationType: ANDA (generic), NDA (new drug application), or BLA (biologics license application)
   - ApplicantName: Name of the applicant/company who applied for making drugs
   - ApprovalDate: Date when applicant got approved for drug manufacturing
   - TECode: Special code referring to ANDA type or drug
   - ApprovedANDACount: Count of generic players or ANDA holders with patent
   - ApprovedANDADetails: Details of generic players or ANDA holders or competitor
   - TAANDACount: Count of companies with tentative approval or without patent
   - TAANDADetails: List of companies with tentative approval
   - DiscontinuedANDACount: Count of discontinued generic players
   - DiscontinuedANDADetails: Details of discontinued generic players
   - PatentCount: Count of patents for a particular product
   - PatentDetails: Details of companies with patents
   - FirstPatentExpiryDate: First patent expiry date
   - LastPatentExperyDate: Last patent expiry date
   - ExclusivityCount: Count of exclusive rights for patents
   - ExclusivityDetails: Details of exclusive rights for patents
   - FirstExclusivityExpiryDate: First expiry date for exclusive rights
   - LastExclusivityExpiryDate: Last expiry date for exclusive rights
   - NCEStatus: Status of 5-year exclusive rights (New Chemical Entity)
   - OBPatentLink: Detailed information about patents
   - ProductLabel: Detailed information about the product
   - NCEDate: Date of NCE rights if applicable
   - DEASStatus: Drug Enforcement Administration status
   - ANDAHolderMarketingAccess: ANDA approval holders with marketing access
   - ProductMarketingAccess: Companies with product marketing access
   - DMFCounts: Count information about Drug Master Files
   - DMFDetails: Detailed information about Drug Master Files
   - PharmaClass: General pharmaceutical class
   - Pharmaclass_EPC: Pharmaceutical class EPC information
   - Pharmaclass_MOA: Pharmaceutical class MOA information
   - ANDAApplicantsCountOwnAPI: Count of ANDA approval holders who use own API/material
   - ANDAApplicantsOwnAPI: Percentage of generic players who use own product for manufacturing
   - ANDAApplicantsDetailsOwnAPI: Details of vertically integrated applicants (own API)
   - ANDAApplicantsDetailsNotOwnAPI: Details of applicants who don't use own API
   - OrphanCode: Orphan drug designation information (values are only 'yes' or 'no')
   - DMFCountsCA: Number of Drug Master Files (DMFs) that have received Complete Assessment (CA) status
   - DMFDetailsCA: A detailed list of Drug Master File (DMF) holders whose DMFs have received Complete Assessment (CA) status


   - NDC: National Drug Code – unique identifier for the drug product and packaging.
   - SUBSTANCENAME: Active ingredient(s) used in the drug.
   - PROPRIETARYNAME: Brand or trade name of the product.
   - DOSAGEFORMNAME: Dosage form of the drug (e.g., Tablet, Injection, Solution).
   - ROUTENAME: Route of administration (e.g., Oral, Intravenous, Topical).
   - ACTIVE_NUMERATOR_STRENGTH: Strength or potency of the active ingredient.
   - ACTIVE_INGRED_UNIT: Unit of measurement for the active ingredient strength (e.g., MG, ML).
   - PACKAGEDESCRIPTION: Description of packaging.
   - LABELERNAME: Name of the company marketing or labeling the drug.
   - APPLICATIONNUMBER: FDA application number associated with the drug (e.g., NDA or ANDA).
   - MARKETINGCATEGORYNAME: Category of marketing authorization (e.g., ANDA, NDA, OTC).
   - STARTMARKETINGDATE: Date when the product began marketing.
   - ENDMARKETINGDATE: Date when the product was discontinued or ended marketing.


   - app_no: Application number assigned to each drug product (e.g., ANDA/NDA number).
   - app_type: Type of application—commonly ANDA (Abbreviated New Drug Application) or NDA (New Drug Application).
   - anda_holder: Name of the company or firm holding the ANDA.
   - activeIngredient: Active pharmaceutical ingredient(s) present in the drug product.
   - te_code: Therapeutic equivalence code indicating substitutability (e.g., AB, BX, etc.).
   - dosage_form: The form or strength of the drug—tablet, injection, cream, capsule, etc.
   - fei_number: FEI (FDA Establishment Identifier) number of the facility.
   - firm_name: Name of the manufacturing firm or drug sponsor.
   - firm_address: Complete address of the manufacturing firm or establishment.
   - pro_marketer_name: Name of the company marketing the product.
   - warning_letter: Indicates if the firm received a warning letter from the FDA.
   - warning_letter_closed_out: Indicates whether the FDA warning letter was closed/resolved.
   - import_alert: Shows if the firm or product is on FDA import alert (e.g., due to GMP violations).
   
   - id: Unique identifier for the manufacturing report entry
   - active_ingredient: Name(s) of the active pharmaceutical ingredient(s) in the product
   - product_brand_name: Brand or trade name of the product
   - form: Dosage form and route of administration (e.g., TABLET;ORAL)
   - dosage: Dosage or strength and unit of the product (e.g., EQ300MGBASE)
   - te_code: Therapeutic Equivalence Code assigned by the FDA
   - product_application_type: Type of FDA application (e.g., ANDA, NDA, BLA)
   - product_application_no: FDA-assigned application number for the product
   - product_applicant_name: Name of the company that submitted the FDA application
   - marketing_status: Marketing category (e.g., Prescription, OTC)
   - product_marketer_name: Name of the company marketing the product
   - UpdateDatetime: Timestamp of the last update to the data record
   - update_report_date: Date on which the report was last generated or refreshed
   """


   # Analyze user question for relevant keywords
   question_lower = user_question.lower()
  
   # Add question-specific guidance based on keyword detection
   question_guidance = ""
   if "generic" in question_lower:
       question_guidance += "\nFor this question about generics, focus on ApprovedANDACount, ApprovedANDADetails columns."
   if "orphan" in question_lower:
       question_guidance += "\nFor this orphan drug question, focus on OrphanCode column which has values 'yes' or 'no'."
   if "patent" in question_lower:
       question_guidance += "\nFor this patent-related question, focus on PatentCount, PatentDetails, FirstPatentExpiryDate, LastPatentExperyDate columns."
   if "tablet" in question_lower or "injection" in question_lower or "form" in question_lower:
       question_guidance += "\nFor this dosage form question, focus on Form column which contains dosage forms."
   if "dea" in question_lower or "scheduled" in question_lower:
       question_guidance += "\nFor this DEA/controlled substance question, focus on DEASStatus column."
   if "market" in question_lower or "marketed" in question_lower:
       question_guidance += "\nFor this marketing-related question, prioritize the fct_product_marketed_by view with product_marketer_name and marketing_status columns."
  
   # Construct a comprehensive prompt with specific schema details
   prompt = f"""Given the following database schema for the view {target_view}:
  
   TARGET VIEW COLUMNS:
   {column_list}   
   {column_descriptions}
  
   The user has asked the following question:
   "{user_question}"
   {question_guidance}
  
   Generate an SQL query to answer the question using ONLY the {target_view} view. Follow these rules:
   1. **If the question asks for a specific name or partial name, use the LIKE operator with % wildcards.** 
   2. **Use only valid column names from the {target_view} view.** 
   3. **Always use the vw_manufacture_report table for any query involving manufacturing, manufacturer, manufacture, etc or specific related terms.** 
   4. **Always use the fct_product_marketed_by table for any query involving marketing, marketed products, etc.**
   5. **Do not escape underscores (_) in table or column names.** 
   6. **Only provide the SQL query as the output. Do not include any additional text or explanations.** 
   7. **Ensure the SQL query is complete and valid.** 
   8. **If the user asks for a specific number of results (e.g., "find the top 5" or "limit to 10"), include a LIMIT clause at the end of the query.** 
   9. **Do not use the TOP keyword, as it is not valid in MySQL.** 
   10. **If the question involves JSON data, ensure the table contains a JSON column.** 
   11. **If the question cannot be answered with this view, skip that view and check with.** 
   12. **VERY IMPORTANT: For ALL "how many" or "count" questions, DO NOT use COUNT(*) functions. Instead, return the LIST OF relevant column that match the criteria.** 
   13. **For questions about specific values or comparisons, use appropriate WHERE clauses.** 
   14. **Always consider NULL values in your conditions - use IS NULL or IS NOT NULL where appropriate.** 
   15. **NEVER use aggregate functions (COUNT, SUM, AVG, etc.) - always return relevant column of data even if the question asks for a count.** 
   16. **For questions that might involve NULL values, use COALESCE or IFNULL to handle them appropriately.** 
   17. **For comparison operations with potentially NULL values, include both the condition and IS NOT NULL check.** 
   18. **Use LEFT JOIN instead of INNER JOIN when you want to preserve all records even if some are NULL.** 
   19. **For text searches, consider using LOWER() for case-insensitive matching.** 
   20. **IMPORTANT: Always return full dataset rows - do not count or aggregate data even if asked to do so.** 
   21. **For semicolon-separated values in columns, use LIKE operations to find partial matches.** 
   22. **For dosage form queries specifically, if looking for 'Tablet' or 'Injection', use appropriate LIKE patterns:** `WHERE Form LIKE '%Tablet%' OR Form LIKE '%Injection%'` 
   23. **For text searches (product names, ingredients, etc.), always use LIKE with wildcards (%)** 
   24. **When searching for partial matches (e.g., "products containing metformin"), use:**`WHERE LOWER(column_name) LIKE '%metformin%'` 
   25. **For questions that might involve partial matches, prefer LIKE over exact matches (=)** 
   26. **CRITICAL: For ANY query containing the word 'generic', you MUST use either ApprovedANDACount (for count/number questions) or ApprovedANDADetails (for information).** 
   27. **For orphan drug queries, OrphanCode can ONLY be 'yes' or 'no' - use exact matches.** 
   28. **MOST IMPORTANT: Only include columns that are explicitly mentioned in the question or are essential for understanding the results.** 
   29. **For example, if the user asks about drugs with tablet form, include columns like BrandName, ActiveIngredient, and Form only, not all columns.** 
   30. **Consider FirstPatentExpiryDate, LastPatentExperyDate, FirstExclusivityExpiryDate, LastExclusivityExpiryDate is None means its null.** 
   31. **Consider FirstPatentExpiryDate, LastPatentExperyDate, FirstExclusivityExpiryDate, LastExclusivityExpiryDate is '0000-00-00' means its null.**
   32. **For ANY query containing the terms 'market', 'marketed', or 'marketing', prioritize the product_marketer_name and marketing_status columns if available.**
   33. **consider all query with distinct column with requested column list.
   """
#30. consider all query with distinct column with requested column list.
   # Call the Groq API with more focused parameters
   try:
       response = client.chat.completions.create(
           model="llama-3.1-8b-instant",
           messages=[
               {"role": "system", "content": "You are a pharmaceutical database SQL expert who generates precise, correct SQL queries that follow all rules. You carefully analyze each question and select only the specific columns needed—never use SELECT *. Read the user's question very carefully and respond strictly according to what is asked."},
               {"role": "user", "content": prompt}
           ],
           max_tokens=300,  # Increased token limit for more complex queries
           temperature=0.2,  # Lower temperature for more deterministic output
       )


       # Extract the generated SQL query
       sql_query = response.choices[0].message.content.strip()
      
       # Check if LLM indicated this view can't answer the question
       if sql_query == "CANNOT_ANSWER_WITH_THIS_VIEW":
           return ""
      
       # Post-process the SQL query to fix escaped underscores and ensure quality
       sql_query = sql_query.replace("\\_", "_")
      
       # Ensure query contains SELECT and FROM clauses
       if "SELECT" not in sql_query.upper() or "FROM" not in sql_query.upper():
           return ""
          
       # Ensure view name is correctly mentioned
       if target_view.lower() not in sql_query.lower():
           # Fix the query to use the correct view
           parts = sql_query.upper().split("FROM")
           if len(parts) > 1:
               sql_query = parts[0] + f" FROM {target_view} " + " ".join(parts[1].split()[1:])
      
       # Check if the query uses SELECT * and replace with a more focused set of columns if so
       if "SELECT *" in sql_query.upper() or "SELECT * " in sql_query.upper():
           # Analyze the question to identify what columns might be needed
           needed_columns = []
          
           # Add default identifier columns
           needed_columns.extend(["BrandName", "ActiveIngredient"])
          
           # Add columns based on keywords in the question
           if "generic" in question_lower:
               needed_columns.extend(["ApprovedANDACount", "ApprovedANDADetails"])
           if "orphan" in question_lower:
               needed_columns.append("OrphanCode")
           if "tablet" in question_lower or "injection" in question_lower or "form" in question_lower:
               needed_columns.append("Form")
           if "patent" in question_lower:
               needed_columns.extend(["PatentCount", "PatentDetails", "FirstPatentExpiryDate", "LastPatentExperyDate"])
           if "dea" in question_lower or "scheduled" in question_lower:
               needed_columns.append("DEASStatus")
           if "dmf" in question_lower or "supplier" in question_lower:
               needed_columns.extend(["DMFCounts", "DMFDetails"])
           if "market" in question_lower or "access" in question_lower or "marketed" in question_lower:
               needed_columns.extend(["product_marketer_name", "marketing_status", "ANDAHolderMarketingAccess", "ProductMarketingAccess"])
          
           # Deduplicate columns
           needed_columns = list(set(needed_columns))
          
           # Validate columns against the schema
           available_columns = schema_info[target_view].keys()
           valid_columns = [col for col in needed_columns if col in available_columns]
          
           # If we have valid columns, replace SELECT * with them
           if valid_columns:
               column_str = ", ".join(valid_columns)
               sql_query = sql_query.upper().replace("SELECT *", f"SELECT {column_str}")
               sql_query = sql_query.upper().replace("SELECT * ", f"SELECT {column_str} ")
      
       return sql_query
      
   except Exception as e:
       st.error(f"Error generating SQL query: {e}")
       return ""


def validate_and_fix_sql_query(sql_query, target_view, schema_info):
   """
   Validates and fixes common issues in the generated SQL query.
   Returns the fixed query or None if the query cannot be fixed.
   """
   if not sql_query:
       return None
      
   try:
       # Convert to uppercase for easier manipulation
       sql_upper = sql_query.upper()
      
       # Check if basic SQL structure is valid
       if "SELECT" not in sql_upper or "FROM" not in sql_upper:
           return None
          
       # Check if target view is correctly included
       if target_view.lower() not in sql_query.lower():
           # Extract SELECT clause
           select_clause = sql_upper.split("FROM")[0]
           # Rebuild query with correct view
           sql_query = f"{select_clause} FROM {target_view}"
           # Add any WHERE clause if it exists
           if "WHERE" in sql_upper:
               where_clause = sql_upper.split("WHERE", 1)[1]
               sql_query += f" WHERE {where_clause}"
      
       # Ensure query ends with semicolon
       if not sql_query.strip().endswith(";"):
           sql_query += ";"
          
       # Check for valid columns in WHERE clause
       if "WHERE" in sql_upper:
           view_columns = schema_info[target_view].keys()
           # Simple validation - ensure column names exist
           # This is a basic check and would need to be more sophisticated for production
          
       return sql_query
          
   except Exception as e:
       st.error(f"Error validating SQL query: {e}")
       return None


def find_answer_in_all_tables(user_question, schema_info):
   """
   Uses LLM to determine best view first, then tries all views in optimal order.
   Returns results, column names, view used, and the SQL query if successful.
   """
   # First, get the suggested view to try first
   best_view = determine_best_view(user_question, schema_info)
  
   # Prepare view order - put the best view first, then others
   views = ["vw_product_detail", "vw_manufacture_report", "vw_ndc_detail_report", "fct_product_marketed_by"]
   
   # Check if the question is about marketing - if so, prioritize fct_product_marketed_by
   question_lower = user_question.lower()
   if "market" in question_lower or "marketed" in question_lower:
       if "fct_product_marketed_by" != best_view:
           views.remove("fct_product_marketed_by")
           view_order = ["fct_product_marketed_by", best_view] + [v for v in views if v != best_view]
       else:
           view_order = [best_view] + [v for v in views if v != best_view]
   else:
       view_order = [best_view] + [v for v in views if v != best_view]
  
   st.info(f"First checking view: {best_view}")
  
   for view in view_order:
       # Generate SQL query for the current view
       with st.spinner(f"Generating query for {view}..."):
           sql_query = generate_sql_query_for_view(user_question, schema_info, view)
      
       # Validate and fix the query if needed
       if sql_query:
           sql_query = validate_and_fix_sql_query(sql_query, view, schema_info)
          
       # If a query was generated, execute it
       if sql_query:
           with st.spinner(f"Executing query on {view}..."):
               results, column_names = execute_sql_query(sql_query)
          
           # If results were found, return them along with the view used
           if results and len(results) > 0:
               return results, column_names, view, sql_query
  
   # If no results were found in any view, return None
   return None, None, None, None


def execute_sql_query(sql_query):
   """
   Executes the generated SQL query against the database and returns the results.
   Handles NULL values properly and includes error handling.
   """
   if not sql_query:
       return None, None


   try:
       # Establishing connection
       conn = mysql.connector.connect(
           host=host,
           user=user,
           password=password,
           database=database
       )


       # Use consume_results parameter to handle unread results
       cursor = conn.cursor(buffered=True)


       # Execute the SQL query
       cursor.execute(sql_query)
      
       # Fetch with row limit to prevent memory issues
       results = cursor.fetchmany(500)  # Limit to 500 rows max for safety


       # Get column names if results were returned
       column_names = [i[0] for i in cursor.description] if cursor.description else []
      
       # Make sure to consume any remaining results to avoid "Unread result found" errors
       while cursor.nextset():
           pass


       # Close the connection
       cursor.close()
       conn.close()


       # Replace None values with 'NULL' for better display
       processed_results = []
       for row in results:
           processed_row = ['NULL' if value is None else value for value in row]
           processed_results.append(processed_row)


       return processed_results, column_names


   except mysql.connector.Error as e:
       st.error(f"Error executing SQL query: {e}")
       st.code(sql_query, language="sql")
       return None, None


def display_query_results(results, column_names, view_used, sql_query):
   """
   Displays the query results in a user-friendly format with NULL handling.
   """


   # Show the successful query
   st.subheader("Successful Query")
   st.code(sql_query, language="sql")
  
   st.subheader("Query Results")
   st.write(f"Found in: {view_used}")
   # Convert results to a DataFrame for better visualization
   df = pd.DataFrame(results, columns=column_names)
   # Replace 'NULL' strings with None for proper display
   df.replace('NULL', None, inplace=True)
   # Display the full DataFrame
   st.write("**Full Results:**")
   st.dataframe(df)


   # Show summary
   st.write(f"Found {len(results)} records matching your query.")
 


def main():
   """
   Main function to interact with the user and execute SQL queries.
   """
   st.title("Pharmaceutical Database Query System")
  
   # Initialize session state for storing the last query
   if "last_query" not in st.session_state:
       st.session_state.last_query = ""
   if "schema_info" not in st.session_state:
       st.session_state.schema_info = None


   # Extract schema information once and store in session state
   if st.session_state.schema_info is None:
       with st.spinner("Loading database schema..."):
           schema_info = extract_schema_info()
           if schema_info:
               st.session_state.schema_info = schema_info
           else:
               st.error("Failed to extract schema information. Please check database connection.")
               return
   else:
       schema_info = st.session_state.schema_info




   # User input for the question with a default placeholder
   user_question = st.text_input(
       "Enter your question about pharmaceutical products:",
       placeholder="e.g., Show me all orphan drugs with tablet dosage form"
   )


   # Add an execution button for better control
   execute_button = st.button("Search Database")


   if user_question and execute_button:
       # Store the query in session state
       st.session_state.last_query = user_question
      
       with st.spinner("Analyzing question and searching for information..."):
           # Search for answer in all tables in order
           results, column_names, view_used, sql_query = find_answer_in_all_tables(user_question, schema_info)


       if results is not None:
           display_query_results(results, column_names, view_used, sql_query)
       else:
           st.warning("No results found in any table for the given question. Please try rephrasing your question.")
           st.info("Tip: Be more specific about what you're looking for (e.g., specify drug name, dosage form, or application type).")


if __name__ == "__main__":
   main()