"""
Test structure template for your sales pipeline.

Start with these basic test patterns and add your own logic.
"""
import pytest
import pandas as pd

class TestDataConservation:
    """Test that data totals are conserved through processing"""
    
    def test_sum_before_and_after_grouping(self, sample_ingram_data, tolerance):
        """Test that grouping doesn't lose or add money"""
        
        # BEFORE grouping - total amount
        original_total = sample_ingram_data['NETAMT'].sum()
        print(f"Original total: ${original_total:.2f}")
        
        # AFTER grouping (your business logic here)
        grouped = sample_ingram_data.groupby(['ISBN', 'NAMECUST']).agg({
            'NETUNITS': 'sum',
            'NETAMT': 'sum'
        }).reset_index()
        
        grouped_total = grouped['NETAMT'].sum()
        print(f"Grouped total: ${grouped_total:.2f}")
        
        # Calculate variance
        if original_total > 0:
            variance = abs(original_total - grouped_total) / original_total
        else:
            variance = 0
            
        print(f"Variance: {variance:.4f} ({variance*100:.2f}%)")
        
        # Your assertion - variance should be less than 5%
        assert variance < tolerance, f"Variance {variance:.4f} exceeds tolerance {tolerance}"
        
    def test_unit_conservation(self, sample_ingram_data, tolerance):
        """Test that unit counts are conserved"""
        # TODO: You write this following the same pattern as above
        pass


class TestMonthlyDataValidation:
    """Test that monthly file data is correct"""
    
    def test_all_records_in_correct_month(self, sample_ingram_data):
        """Test that all records belong to the expected month"""
        
        expected_month = 10  # October
        expected_year = 2024
        
        # Check all records are from expected month
        wrong_month = sample_ingram_data[
            (sample_ingram_data['MONTH'] != expected_month) |
            (sample_ingram_data['YEAR'] != expected_year)
        ]
        
        print(f"Records in wrong month/year: {len(wrong_month)}")
        
        # Should have no records in wrong month
        assert len(wrong_month) == 0, f"Found {len(wrong_month)} records in wrong month/year"
        
    def test_no_null_critical_fields(self, sample_ingram_data):
        """Test that critical fields have no null values"""
        
        critical_fields = ['ISBN', 'NETAMT', 'NETUNITS', 'NAMECUST']
        
        for field in critical_fields:
            null_count = sample_ingram_data[field].isna().sum()
            print(f"Null values in {field}: {null_count}")
            assert null_count == 0, f"Found {null_count} null values in {field}"


class TestBusinessRules:
    """Test your specific business logic"""
    
    def test_no_negative_amounts(self, sample_ingram_data):
        """Test that we don't have negative sales amounts"""
        
        negative_amounts = sample_ingram_data[sample_ingram_data['NETAMT'] < 0]
        
        print(f"Negative amounts found: {len(negative_amounts)}")
        
        assert len(negative_amounts) == 0, "No negative amounts allowed in sales data"
        
    def test_isbn_format(self, sample_ingram_data):
        """Test that ISBNs are properly formatted"""
        
        # Remove any formatting and check length
        clean_isbns = sample_ingram_data['ISBN'].str.replace('-', '').str.replace(' ', '')
        
        # Should be 10 or 13 digits
        invalid_isbns = clean_isbns[~clean_isbns.str.len().isin([10, 13])]
        
        print(f"Invalid ISBN formats: {len(invalid_isbns)}")
        
        assert len(invalid_isbns) == 0, f"Found {len(invalid_isbns)} invalid ISBN formats"


# Example of testing your actual pipeline function
class TestPipelineIntegration:
    """Test your actual pipeline functions"""
    
    def test_combined_sales_with_mock_db(self, polars_sample_ingram, polars_sample_sage, mock_database_engine):
        """
        Test your actual combined_sales_report function with mock database
        """
        # Import your real function
        from pipelines.combined_sales_report import combined_sales_report
        
        # Call your real function with test data and mock database
        try:
            result = combined_sales_report(polars_sample_ingram, polars_sample_sage, mock_database_engine)
            
            # Test the result
            assert result is not None, "Function should return a result"
            print(f"Result type: {type(result)}")
            print(f"Result shape: {result.shape if hasattr(result, 'shape') else 'No shape'}")
            
            # Add your specific validations here
            # For example:
            # assert 'NETAMT' in result.columns
            # assert len(result) > 0
            
        except Exception as e:
            print(f"Function call failed: {e}")
            # You might want to assert False here or handle the error appropriately
    
    @pytest.mark.integration  # Mark this as an integration test
    def test_combined_sales_with_real_db(self, polars_sample_ingram, polars_sample_sage, real_database_engine):
        """
        Test with real database connection (only runs if DB is available)
        """
        from pipelines.combined_sales_report import combined_sales_report
        
        # This will only run if real_database_engine fixture succeeds
        result = combined_sales_report(polars_sample_ingram, polars_sample_sage, real_database_engine)
        
        assert result is not None
        # Add more specific tests for real DB interaction
    
    def test_database_connection_simple(self, real_database_engine):
        """Simple test to verify database connection works"""
        import pandas as pd
        
        # Simple query to test connection
        result = pd.read_sql("SELECT 1 as test_value", real_database_engine)
        
        assert len(result) == 1
        assert result['test_value'].iloc[0] == 1
        print("Database connection successful!")


if __name__ == "__main__":
    # This lets you run the file directly: python test_sales_pipeline.py
    pytest.main([__file__, "-v"])
