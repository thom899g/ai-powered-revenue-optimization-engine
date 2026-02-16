import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from loguru import logger

logger.configure(
    handlers=[dict(
        sink=sys.stderr,
        format="[{level}] {message}",
        level="INFO"
    )]
)

class MarketTrendAnalyzer:
    def __init__(self):
        self.data_feed: Optional[pd.DataFrame] = None
        
    def update_data(self, data: pd.DataFrame) -> None:
        """Updates the internal data feed with new market trends."""
        self.data_feed = data
        logger.info("Market data updated successfully.")
        
    def _get_trend_data(
        self, 
        time_period: str,
        window_size: int
    ) -> Dict[str, Any]:
        """
        Computes trend metrics for a given time period and window size.
        Args:
            time_period: The period to analyze (e.g., '7D', '30D').
            window_size: Number of periods to consider.
        Returns:
            A dictionary containing the computed trends.
        Raises:
            ValueError: If data is not available for the specified period.
        """
        if self.data_feed is None or len(self.data_feed) < window_size:
            raise ValueError("Insufficient data available.")
            
        end_date = datetime.now()
        start_date = end_date - timedelta(**{time_period})
        
        filtered_data = self.data_feed[
            (self.data_feed['date'] >= start_date) &
            (self.data_feed['date'] <= end_date)
        ]
        
        if len(filtered_data) == 0:
            raise ValueError("No data available for the specified period.")
            
        trend_metrics = {
            'average_growth': filtered_data['revenue'].mean(),
            'max_revenue': filtered_data['revenue'].max(),
            'min_revenue': filtered_data['revenue'].min()
        }
        
        return trend_metrics
    
    def analyze_trends(
        self,
        time_period: str = '7D',
        window_size: int = 30
    ) -> Dict[str, Any]:
        """
        Analyzes market trends over a specified period and window size.
        Args:
            time_period: The period to analyze (e.g., '7D', '30D').
            window_size: Number of periods to consider.
        Returns:
            A dictionary containing the computed trend metrics.
        """
        try:
            trend_data = self._get_trend_data(time_period, window_size)
            logger.info("Trend analysis completed successfully.")
            return trend_data
        except Exception as e:
            logger.error(f"Error in trend analysis: {str(e)}")
            raise