"""
PII Masking - Silver Layer
Masks PII fields in silver layer for privacy compliance
IP addresses and geo-location data derived from IP should be masked
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, regexp_replace, split, concat, trim
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class PIIMaskingError(PipelineException):
    """Raised when PII masking fails"""
    pass


class PIIMasker:
    """
    Production-grade PII masker for silver layer
    
    Masks PII fields for privacy compliance:
    - IP addresses: anonymize last octet (192.168.1.XXX → 192.168.1.0)
    - Geo-location (country, region, timezone): mask/empty (derived from IP)
    - User segment: not PII, but may be masked if required
    
    Example:
        masker = PIIMasker()
        masked_df = masker.mask_pii(df)
        # PII masked, privacy compliant
    """
    
    def __init__(
        self,
        config_path: str = "config/spark_config.yaml",
        mask_mode: str = "anonymize"
    ):
        """
        Initialize PII masker
        
        Args:
            config_path: Path to config file
            mask_mode: "anonymize" (mask last octet) or "remove" (set to null)
        """
        self.mask_mode = mask_mode
        
        # Load config
        try:
            self.config = load_config(config_path)
            pii_config = self.config.get("pii_handling", {}).get("silver", {})
            self.mask_mode = pii_config.get("mask_mode", mask_mode)
            self.mask_ip = pii_config.get("mask_ip_address", True)
            self.mask_geo = pii_config.get("mask_geo_location", True)
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.mask_ip = True
            self.mask_geo = True
        
        logger.info(f"PII masker initialized (mode: {self.mask_mode}, mask_ip: {self.mask_ip}, mask_geo: {self.mask_geo})")
    
    def mask_pii(
        self,
        df: DataFrame,
        mark_masked: bool = True
    ) -> DataFrame:
        """
        Mask PII fields in DataFrame
        
        Args:
            df: Input DataFrame
            mark_masked: Add column indicating PII was masked
        
        Returns:
            DataFrame with PII masked
        
        Example:
            masker = PIIMasker()
            masked_df = masker.mask_pii(df)
        """
        try:
            logger.info("Starting PII masking for silver layer...")
            
            df_result = df
            
            # Mask IP address
            if self.mask_ip and "ip_address" in df_result.columns:
                df_result = self._mask_ip_address(df_result)
                logger.info("Masked IP addresses")
            
            # Mask geo-location fields (derived from IP, so should be masked)
            if self.mask_geo:
                geo_fields = ["country", "region", "timezone"]
                for field in geo_fields:
                    if field in df_result.columns:
                        df_result = self._mask_geo_field(df_result, field)
                logger.info("Masked geo-location fields")
            
            # Mark as masked if requested
            if mark_masked and "pii_masked" not in df_result.columns:
                df_result = df_result.withColumn("pii_masked", lit(True))
            
            logger.info("PII masking complete")
            return df_result
            
        except Exception as e:
            logger.error(f"PII masking failed: {str(e)}", exc_info=True)
            raise PIIMaskingError(f"Cannot mask PII: {str(e)}")
    
    def _mask_ip_address(self, df: DataFrame) -> DataFrame:
        """
        Mask IP address (anonymize last octet)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with IP addresses masked
        """
        try:
            if self.mask_mode == "anonymize":
                # Anonymize: replace last octet with 0 (e.g., 192.168.1.123 → 192.168.1.0)
                # Split IP, take first 3 octets, append .0
                df_result = df.withColumn(
                    "ip_address",
                    when(
                        col("ip_address").isNotNull() & (trim(col("ip_address")) != ""),
                        concat(
                            split(col("ip_address"), "\\.")[0], lit("."),
                            split(col("ip_address"), "\\.")[1], lit("."),
                            split(col("ip_address"), "\\.")[2], lit(".0")
                        )
                    ).otherwise(lit(None).cast("string"))
                )
            elif self.mask_mode == "remove":
                # Remove: set to null
                df_result = df.withColumn("ip_address", lit(None).cast("string"))
            else:
                # Default: anonymize
                df_result = df.withColumn(
                    "ip_address",
                    when(
                        col("ip_address").isNotNull() & (trim(col("ip_address")) != ""),
                        regexp_replace(col("ip_address"), r"\.\d+$", ".0")
                    ).otherwise(lit(None).cast("string"))
                )
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error masking IP address: {str(e)}", exc_info=True)
            raise PIIMaskingError(f"Cannot mask IP address: {str(e)}")
    
    def _mask_geo_field(self, df: DataFrame, field_name: str) -> DataFrame:
        """
        Mask geo-location field (set to null for privacy)
        
        Args:
            df: Input DataFrame
            field_name: Field name to mask (country, region, timezone)
        
        Returns:
            DataFrame with geo field masked
        """
        try:
            # For privacy, geo-location derived from IP should be masked/removed
            # Set to null (empty) since it's PII-related
            df_result = df.withColumn(field_name, lit(None).cast("string"))
            logger.debug(f"Masked geo-location field: {field_name}")
            return df_result
            
        except Exception as e:
            logger.error(f"Error masking geo field {field_name}: {str(e)}", exc_info=True)
            raise PIIMaskingError(f"Cannot mask geo field {field_name}: {str(e)}")


# Convenience function
def mask_pii_fields(df: DataFrame, mask_mode: str = "anonymize") -> DataFrame:
    """
    Mask PII fields in DataFrame (convenience function)
    
    Args:
        df: Input DataFrame
        mask_mode: "anonymize" or "remove"
    
    Returns:
        DataFrame with PII masked
    
    Example:
        masked_df = mask_pii_fields(df)
    """
    masker = PIIMasker(mask_mode=mask_mode)
    return masker.mask_pii(df)

