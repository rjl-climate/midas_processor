### **Summary: MIDAS Quality Control (Section 5) for an AI Agent**

This summary outlines the key concepts, data structures, and logic required for an AI agent to correctly interpret and use the quality control (QC) information within the Met Office MIDAS dataset.

#### **1. Core Philosophy of MIDAS QC**

*   **Purpose:** The primary goal of QC is not to delete data, but to provide users (or an AI) with information to assess if a specific observation is **"fit for purpose."**
*   **Key Takeaway:** Data flagged as "suspect" is not necessarily "bad." It simply failed one or more automated or manual checks. The application should allow for nuanced handling of this data, rather than simply discarding it.

#### **2. The Two-Level QC Process**

The QC process is performed in two distinct stages. An AI should understand the difference as it implies the rigor of the checks applied.

*   **Level 1 QC (Automated, Near Real-Time):**
    *   **When:** Applied automatically as data is ingested.
    *   **What it Checks:**
        *   **Fixed Limits:** Checks if values are physically plausible (e.g., relative humidity between 0-100%, rainfall is not negative).
        *   **Internal Consistency:** Checks for logical consistency within a single observation report (e.g., minimum temperature cannot be greater than maximum temperature).
        *   **Rate of Change:** Checks for unrealistic "jumps" in values between consecutive reports from the same station.
        *   **Climatological Limits:** Checks if a value is plausible for the specific location and time of year by comparing it to long-term climate records.

*   **Level 2 QC (Detailed, Non-Real-Time):**
    *   **When:** Performed later, often with human oversight, when more data is available for context.
    *   **What it Checks:**
        *   **Spatial Consistency:** Compares a station's observation with those from nearby stations. An outlier might be flagged.
        *   **Temporal Consistency:** Looks for suspicious patterns or inconsistencies over a longer time series for a single station.
        *   **Manual Review:** A human expert reviews data flagged by the automated systems to make a final determination.

#### **3. How to Parse and Interpret QC Data in Your App**

The QC information is provided in specific columns within the MIDAS data files. Your agent must be designed to parse these.

**Key Data Fields:**

1.  **Quality Indicator Flag (The most important field)**
    *   **Column Naming Convention:** A dedicated column for each measurement, prefixed with `q_`. For example, `air_temperature` data will have a corresponding `q_air_temperature` column.
    *   **Value Mapping (Essential Logic):** The agent must interpret the integer code in this column as follows:
        *   `0` = **Valid / Passed:** The data has passed all QC checks it was subjected to. **(This is the highest quality data).**
        *   `1` = **Suspect:** The data failed at least one QC check. It should be used with caution. The application could offer users the option to include or exclude this data.
        *   `2` = **Erroneous:** The data is considered incorrect and should not be used for most purposes.
        *   `3` = **No QC Applied:** The data has not been processed by the QC system.
        *   `9` = **Missing Data:** No quality information is available because the source data itself is missing.

2.  **QC Version Number (`qc_version_num`)**
    *   **What it is:** An integer representing the version of the QC methodology used.
    *   **Why it Matters:** The QC tests and rules can change over time. If processing historical data, the agent should be aware that data with different `qc_version_num` values may have been checked against different standards.

3.  **Source of QC (`meto_qc_code`)**
    *   **What it is:** A code indicating which organization performed the QC (e.g., `'M'` for Met Office, `'E'` for Environment Agency).
    *   **Why it Matters:** Different sources may have different QC standards. This allows for more granular data filtering if needed.

4.  **Specific QC Test Names**
    *   **How it Works:** The system tracks which specific test(s) a "suspect" value failed (e.g., "Rate of Change check").
    *   **Location:** This detailed information is not in the main data table but is referenced in **Appendix B** of the user guide. For advanced applications, the agent could be programmed with the list of tests from Appendix B to provide users with specific reasons why a data point was flagged.

---

### **Recommended Logic for the AI Agent's App**

1.  **Default Behavior:** By default, the application should process or display only data where the relevant `q_` flag is `0` (Valid).
2.  **User-Configurable Tolerance:** Provide a setting that allows the user to change the quality threshold. For example:
    *   **Strict:** Use `q_` flag = `0` only.
    *   **Standard:** Use `q_` flags `0` and `1` (Valid and Suspect).
    *   **All Data:** Use all data, but visually distinguish or annotate data based on its QC flag.
3.  **Data Ingestion:** The agent's data parser **must** identify and process all columns prefixed with `q_` to link quality flags to their corresponding measurements.
4.  **Error Handling & Visualization:**
    *   When a user queries or visualizes data, the application should be able to color-code or annotate points based on their QC flag (`Valid`, `Suspect`, `Erroneous`).
    *   For an advanced feature, if a point is `Suspect`, the app could provide the specific reason for the flag by cross-referencing the information from Appendix B.
