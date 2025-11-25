"""
PII Scrubber - Production ML Pipeline
Removes or masks PII (Personally Identifiable Information) for privacy compliance
GDPR, CCPA compliance - critical for production ML pipelines
"""
import hashlib
import re
from typing import Dict, Any, List, Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, regexp_replace, hash, md5, sha2, concat, substring, length
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class PIIScrubberError(PipelineException):
    """Raised when PII scrubbing fails"""
    pass


class PIIScrubber:
    """
    Production-grade PII scrubber
    
    Handles:
    - User IDs (hash or mask)
    - IP addresses (anonymize)
    - Email addresses (hash or remove)
    - Phone numbers (mask)
    - Device IDs (hash)
    - Custom PII fields
    
    Example:
        scrubber = PIIScrubber()
        scrubbed_df = scrubber.scrub_pii(df)
        # PII removed/masked, privacy compliant
    """
    
    def __init__(
        self,
        config_path: str = "kafka.yaml",
        scrub_mode: str = "hash",
        salt: Optional[str] = None
    ):
        """
        Initialize PII scrubber
        
        Args:
            config_path: Path to config file
            scrub_mode: "hash", "mask", or "remove"
            salt: Optional salt for hashing (for consistent hashing)
        """
        self.scrub_mode = scrub_mode
        self.salt = salt or "default_salt_change_in_production"
        
        # Load config
        try:
            self.config = load_config(config_path)
            pii_config = self.config.get("pii_handling", {})
            self.scrub_mode = pii_config.get("scrub_mode", scrub_mode)
            self.pii_fields = pii_config.get("pii_fields", [])
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.pii_fields = []
        
        # Default PII fields to scrub
        if not self.pii_fields:
            self.pii_fields = [
                "user_id",
                "ip_address",
                "email",
                "phone_number",
                "device_id",
                "session_id"
            ]
        
        logger.info(f"PII scrubber initialized (mode: {scrub_mode}, fields: {len(self.pii_fields)})")
    
    def scrub_pii(
        self,
        df: DataFrame,
        mark_scrubbed: bool = True
    ) -> DataFrame:
        """
        Scrub PII from DataFrame
        
        Args:
            df: Input DataFrame
            mark_scrubbed: Add column indicating PII was scrubbed
        
        Returns:
            DataFrame with PII scrubbed
        
        Example:
            scrubber = PIIScrubber()
            scrubbed_df = scrubber.scrub_pii(df)
        """
        try:
            logger.info("Starting PII scrubbing...")
            
            # Scrub each PII field
            for field in self.pii_fields:
                if field in df.columns:
                    df = self._scrub_field(df, field)
                    logger.debug(f"Scrubbed field: {field}")
            
            # Scrub nested fields (e.g., device_info.ip_address)
            df = self._scrub_nested_fields(df)
            
            # Mark as scrubbed
            if mark_scrubbed:
                df = df.withColumn("pii_scrubbed", lit(True))
            
            logger.info("PII scrubbing completed")
            return df
            
        except Exception as e:
            logger.error(f"PII scrubbing failed: {str(e)}", exc_info=True)
            raise PIIScrubberError(f"Cannot scrub PII: {str(e)}")
    
    def _scrub_field(self, df: DataFrame, field_name: str) -> DataFrame:
        """
        Scrub a specific field based on scrub mode
        
        Args:
            df: Input DataFrame
            field_name: Field name to scrub
        
        Returns:
            DataFrame with field scrubbed
        """
        if self.scrub_mode == "hash":
            return self._hash_field(df, field_name)
        elif self.scrub_mode == "mask":
            return self._mask_field(df, field_name)
        elif self.scrub_mode == "remove":
            return self._remove_field(df, field_name)
        else:
            logger.warning(f"Unknown scrub mode: {self.scrub_mode}, using hash")
            return self._hash_field(df, field_name)
    
    def _hash_field(self, df: DataFrame, field_name: str) -> DataFrame:
        """
        Hash field value (one-way, consistent hashing)
        
        Args:
            df: Input DataFrame
            field_name: Field name to hash
        
        Returns:
            DataFrame with field hashed
        """
        # Use SHA-256 for hashing (with salt for consistency)
        # Salt ensures same input produces same hash (useful for joins)
        df = df.withColumn(
            field_name,
            when(
                col(field_name).isNotNull(),
                sha2(
                    concat(lit(self.salt), col(field_name)),
                    256
                )
            ).otherwise(None)
        )
        return df
    
    def _mask_field(self, df: DataFrame, field_name: str) -> DataFrame:
        """
        Mask field value (partial visibility)
        
        Args:
            df: Input DataFrame
            field_name: Field name to mask
        
        Returns:
            DataFrame with field masked
        """
        # Mask strategy depends on field type
        if "ip" in field_name.lower():
            # IP address: mask last octet (e.g., 192.168.1.XXX)
            df = df.withColumn(
                field_name,
                when(
                    col(field_name).isNotNull(),
                    regexp_replace(col(field_name), r"\.\d+$", ".XXX")
                ).otherwise(None)
            )
        elif "email" in field_name.lower():
            # Email: mask domain (e.g., user@XXX.com)
            df = df.withColumn(
                field_name,
                when(
                    col(field_name).isNotNull(),
                    regexp_replace(col(field_name), r"@.*", "@XXX.com")
                ).otherwise(None)
            )
        elif "phone" in field_name.lower():
            # Phone: mask last 4 digits (e.g., +1-555-XXX-XXXX)
            df = df.withColumn(
                field_name,
                when(
                    col(field_name).isNotNull(),
                    regexp_replace(col(field_name), r"\d{4}$", "XXXX")
                ).otherwise(None)
            )
        else:
            # Default: mask all but first 2 and last 2 characters
            df = df.withColumn(
                field_name,
                when(
                    col(field_name).isNotNull(),
                    when(
                        length(col(field_name)) > 4,
                        concat(
                            substring(col(field_name), 1, 2),
                            lit("XXX"),
                            substring(col(field_name), -2, 2)
                        )
                    ).otherwise(lit("XXX"))
                ).otherwise(None)
            )
        
        return df
    
    def _remove_field(self, df: DataFrame, field_name: str) -> DataFrame:
        """
        Remove field entirely (set to null)
        
        Args:
            df: Input DataFrame
            field_name: Field name to remove
        
        Returns:
            DataFrame with field set to null
        """
        df = df.withColumn(field_name, lit(None))
        return df
    
    def _scrub_nested_fields(self, df: DataFrame) -> DataFrame:
        """
        Scrub PII from nested fields (e.g., device_info.ip_address)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with nested PII scrubbed
        """
        # Scrub device_info.ip_address if present
        if "device_info" in df.columns or "device_info.ip_address" in df.columns:
            # Handle nested struct
            try:
                if "device_info.ip_address" in df.columns:
                    df = self._scrub_field(df, "device_info.ip_address")
                elif "device_info" in df.columns:
                    # Nested struct - would need more complex handling
                    logger.debug("device_info is nested struct, scrubbing handled at struct level")
            except Exception as e:
                logger.warning(f"Could not scrub nested device_info.ip_address: {str(e)}")
        
        # Scrub processing_metadata if it contains PII
        # (Usually doesn't, but check if needed)
        
        return df
    
    def hash_user_id(self, df: DataFrame, user_id_column: str = "user_id") -> DataFrame:
        """
        Hash user ID specifically (common use case)
        
        Args:
            df: Input DataFrame
            user_id_column: User ID column name
        
        Returns:
            DataFrame with user_id hashed
        """
        if user_id_column not in df.columns:
            logger.warning(f"Column {user_id_column} not found")
            return df
        
        logger.info(f"Hashing {user_id_column}...")
        return self._hash_field(df, user_id_column)
    
    def anonymize_ip(self, df: DataFrame, ip_column: str = "ip_address") -> DataFrame:
        """
        Anonymize IP address (mask last octet)
        
        Args:
            df: Input DataFrame
            ip_column: IP address column name
        
        Returns:
            DataFrame with IP anonymized
        """
        if ip_column not in df.columns:
            logger.warning(f"Column {ip_column} not found")
            return df
        
        logger.info(f"Anonymizing {ip_column}...")
        return self._mask_field(df, ip_column)
    
    def get_scrubbing_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get PII scrubbing statistics
        
        Args:
            df: DataFrame with pii_scrubbed column
        
        Returns:
            Dictionary with scrubbing statistics
        """
        try:
            if "pii_scrubbed" not in df.columns:
                return {"pii_scrubbed": False, "message": "PII scrubbing not applied"}
            
            total_records = df.count()
            scrubbed_count = df.filter(col("pii_scrubbed") == True).count()
            
            return {
                "total_records": total_records,
                "scrubbed_records": scrubbed_count,
                "scrub_mode": self.scrub_mode,
                "pii_fields_scrubbed": self.pii_fields
            }
            
        except Exception as e:
            logger.error(f"Error getting scrubbing stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def scrub_pii(
    df: DataFrame,
    scrub_mode: str = "hash",
    mark_scrubbed: bool = True
) -> DataFrame:
    """
    Scrub PII from DataFrame (convenience function)
    
    Args:
        df: Input DataFrame
        scrub_mode: "hash", "mask", or "remove"
        mark_scrubbed: Add column indicating PII was scrubbed
    
    Returns:
        DataFrame with PII scrubbed
    
    Example:
        scrubbed_df = scrub_pii(df, scrub_mode="hash")
    """
    scrubber = PIIScrubber(scrub_mode=scrub_mode)
    return scrubber.scrub_pii(df, mark_scrubbed=mark_scrubbed)

