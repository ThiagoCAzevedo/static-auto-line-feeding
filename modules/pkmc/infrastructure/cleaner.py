from .base import PKMCBase
import polars as pl


class PKMCCleaner(PKMCBase):
    def rename_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        rename_map = {
            "Material": "partnumber",
            "Área abastec.prod.": "supply_area",
            "Nº circ.regul.": "num_reg_circ",
            "Tipo de depósito": "deposit_type",
            "Posição no depósito": "deposit_position",
            "Container": "container",
            "Texto breve de material": "description",
            "Norma de embalagem": "pack_standard",
            "Quantidade Kanban": "qty_per_box", 
            "Posição de armazenamento": "qty_max_box",
        }

        try:
            df = self.rename(df, rename_map)
            return df
        except Exception as e:
            self.log.error(f"Column rename failed: {str(e)}", exc_info=True)
            raise
    
    def filter_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        try:
            df = df.filter(pl.col("deposit_type") == "B01")
            return df
        except Exception as e:
            self.log.error(f"Column filter failed: {str(e)}", exc_info=True)
            raise
    
    def clean_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        try:
            df = df.with_columns(
                pl.col("qty_max_box")
                    .cast(pl.Utf8)
                    .str.replace_all(r"(?i)max", "")
                    .str.replace_all(r"[ :]", "")
                    .str.replace_all(r"\D+", "")
                    .cast(pl.Int64, strict=False)
                    .fill_null(0),

                pl.col("partnumber")
                    .cast(pl.Utf8)
                    .str.strip()         
                    .str.replace_all(r"\s+", "")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(r"[^\w-]", "")
                    .str.to_uppercase()
            )
            return df
        except Exception as e:
            self.log.error(f"Data cleaning failed: {str(e)}", exc_info=True)
            raise

    def create_columns(self, df: pl.LazyFrame) -> pl.LazyFrame:
        try:
            df = df.with_columns([
                (pl.col("qty_per_box") * pl.col("qty_max_box")).alias("total_theoretical_qty"),
                (pl.col("qty_per_box") * (pl.col("qty_max_box") - 1)).alias("qty_for_restock"),
                pl.lit(2000).alias("lb_balance"),
                pl.col("supply_area").str.extract(r"(P\d+[A-Z]?)", 0).alias("rack")
            ])

            df = df.with_columns([
                (pl.col("lb_balance") / (pl.col("qty_per_box") - 1))
                    .round(2)
                    .alias("lb_balance_box")
            ])

            df = df.drop_nulls("rack")

            return df
        except Exception as e:
            self.log.error(f"Column creation failed: {str(e)}", exc_info=True)
            raise