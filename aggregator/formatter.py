import pandas as pd
import json
import io
import re
import fnmatch
import logging

logger = logging.getLogger(__name__)

class DataFormatter:
    def __init__(self, mappings_df):
        """
        mappings_df: DataFrame with columns:
        [unified_name, orchard_col, nextone_col, itunes_col, is_date, is_numeric]
        """
        self.mappings = mappings_df

    def detect_source(self, filename, content_str=None):
        # 1. Content-based detection (Highest priority)
        if content_str:
            source = self._detect_source_from_content(content_str)
            if source != "UNKNOWN":
                return source

        # 2. Filename-based detection (Fallback)
        fn = filename.lower()
        if fnmatch.fnmatch(fn, "orchard*"): return "ORCHARD"
        if fnmatch.fnmatch(fn, "divsiteall*") or "music-sales" in fn: return "NEXTONE"
        if "_zz" in fn: return "ITUNES"
        
        return "UNKNOWN"

    def _detect_source_from_content(self, content_str):
        if not content_str:
            return "UNKNOWN"
        
        # Check first 2000 chars for efficiency
        header_sample = content_str[:2000]
        
        # Apple Music detection
        if "Report Type" in header_sample and ("Apple Music" in header_sample or "Net Royalty Total" in header_sample):
            return "ITUNES"
            
        # NexTone detection (Domestic TSV)
        # Check for characteristic Japanese headers
        nextone_keywords = ["分配月", "利用月", "原盤/アルバムコード", "使用料合計"]
        if any(k in header_sample for k in nextone_keywords):
            return "NEXTONE"
            
        # Orchard detection
        orchard_keywords = ["STATEMENT PERIOD", "ACCOUNT ID", "NET SHARE ACCOUNT CURRENCY", "PRODUCT ARTIST"]
        if any(k in header_sample for k in orchard_keywords):
            return "ORCHARD"
            
        return "UNKNOWN"

    def format_file(self, file_io, filename, source_type=None):
        logger.info(f"Start parsing file: {filename}")
        
        # Read a small part first for detection
        file_io.seek(0)
        sample_bytes = file_io.read(4000) # Read enough for headers
        sample_str = ""
        for enc in ['utf-8-sig', 'utf-8', 'cp932', 'shift-jis', 'latin1']:
            try:
                sample_str = sample_bytes.decode(enc)
                break
            except Exception:
                pass

        if not source_type or source_type == "UNKNOWN":
            source_type = self.detect_source(filename, sample_str)
        
        logger.info(f"Detected source: {source_type} for {filename}")
        df = self._read_raw_to_df(file_io, source_type)
        
        if df is None or df.empty:
            logger.warning(f"File {filename} parsed empty.")
            return None
            
        logger.info(f"Parsed {len(df)} rows. Applying mapping...")
        return self._apply_mapping(df, source_type, filename)
        
    def _read_raw_to_df(self, file_io, source_type):
        file_io.seek(0)
        content_bytes = file_io.read()
        
        content_str = None
        for enc in ['utf-8-sig', 'utf-8', 'cp932', 'shift-jis', 'latin1']:
            try:
                content_str = content_bytes.decode(enc)
                break
            except Exception:
                pass
                
        if not content_str:
            return None
            
        lines = content_str.splitlines()
        
        if source_type == "ITUNES":
            # Apple Music: Extract End Date
            end_date = None
            start_row = 0
            for i, line in enumerate(lines[:30]):
                if "End Date" in line:
                    match = re.search(r'End Date[\s:]*([A-Za-z0-9/]+)', line)
                    if match:
                        end_date = match.group(1).strip()
                # Guess header row if we see enough tabs/commas
                if line.count('\t') >= 5 or line.count(',') >= 5:
                    start_row = i
                    break
            
            # Count separators in header
            if len(lines) > start_row:
                sep = '\t' if lines[start_row].count('\t') > lines[start_row].count(',') else ','
            else:
                sep = '\t'
            df = pd.read_csv(io.StringIO(content_str), sep=sep, skiprows=start_row, low_memory=False)
            if end_date:
                df['_extracted_end_date'] = end_date
            return df
            
        elif source_type == "NEXTONE":
            # Domestic TSV, skip row 1
            df = pd.read_csv(io.StringIO(content_str), sep='\t', skiprows=1, low_memory=False)
            return df
            
        elif source_type == "ORCHARD":
            # Orchard CSV
            df = pd.read_csv(io.StringIO(content_str), sep=',', low_memory=False)
            return df
            
        else:
            # Fallback auto-detect separated by tab or comma
            if content_str.count('\t') > content_str.count(','):
                sep = '\t'
            else:
                sep = ','
            return pd.read_csv(io.StringIO(content_str), sep=sep, low_memory=False)

    def _apply_mapping(self, df, source_type, filename):
        src_col_key = {
            "ORCHARD": "orchard_col",
            "NEXTONE": "nextone_col",
            "ITUNES": "itunes_col"
        }.get(source_type, "orchard_col")
        
        df.columns = [str(c).strip() for c in df.columns]
        consumed_cols = set()
        
        new_df = pd.DataFrame(index=df.index)
        
        for _, row in self.mappings.iterrows():
            u_name = str(row['unified_name']).strip()
            s_name = str(row[src_col_key]).strip()
            
            if s_name in df.columns:
                val = df[s_name].copy()
                consumed_cols.add(s_name)
            elif u_name == "売上確定日" and '_extracted_end_date' in df.columns:
                val = df['_extracted_end_date'].copy()
                consumed_cols.add('_extracted_end_date')
            else:
                val = pd.Series([None] * len(df), index=df.index)
                
            if row['is_numeric']:
                if val.dtype == 'object':
                    val = val.astype(str).str.replace(',', '', regex=False)
                val = pd.to_numeric(val, errors='coerce').fillna(0)
            elif row['is_date']:
                val = self._normalize_date(val)
            else:
                val = val.fillna("").astype(str)
                val = val.replace("nan", "")
                
            new_df[u_name] = val
        
        # Any remaining columns go to JSON in "備考"
        # However, checking every row could be slow for big files (1GB). Let's optimize if possible,
        # but apply is generally okay if unmapped_cols is empty.
        unmapped_cols = [c for c in df.columns if c not in consumed_cols and c != '_extracted_end_date']
        
        if '備考' not in new_df.columns:
            new_df['備考'] = ""

        if unmapped_cols:
            def build_json(r):
                d = {c: str(r[c]) for c in unmapped_cols if pd.notna(r[c]) and str(r[c]).strip() != ""}
                return json.dumps(d, ensure_ascii=False) if d else ""
            
            # Add to existing 備考 if any
            extra_info = df.apply(build_json, axis=1)
            mask = extra_info != ""
            new_df.loc[mask, '備考'] = new_df.loc[mask, '備考'] + " " + extra_info.loc[mask]

        new_df['FILE_NAME'] = filename
        new_df['SOURCE'] = source_type
        
        return new_df
        
    def _normalize_date(self, series):
        s = series.astype(str).str.strip().replace("nan", "")
        mask_yyyymm = s.str.match(r'^\d{6}$')
        if mask_yyyymm.any():
             s.loc[mask_yyyymm] = s.loc[mask_yyyymm].str[:4] + "-" + s.loc[mask_yyyymm].str[4:6] + "-01"
             
        dt_series = pd.to_datetime(s, errors='coerce')
        return dt_series.dt.strftime('%Y-%m-01').fillna(s)
