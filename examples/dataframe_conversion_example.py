#!/usr/bin/env python3
"""
Example: DataFrame Conversion

This example demonstrates how to dynamically convert any API response to pandas DataFrames
for data analysis and manipulation.

Features demonstrated:
- Convert various API responses to DataFrames
- Handle nested objects and complex data structures
- Automatic data type detection and conversion
- Custom transformations and filtering
- Specialized conversion methods for different response types
"""

import asyncio
import sys
from pathlib import Path
import warnings

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import DataQueryError, AuthenticationError

# Suppress pandas warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)


async def basic_dataframe_conversion_example():
    """Demonstrate basic DataFrame conversion functionality."""
    print("🔄 Basic DataFrame Conversion Example")
    print("=" * 60)
    
    try:
        # Check if pandas is available
        try:
            import pandas as pd
            print("✅ pandas is available")
        except ImportError:
            print("❌ pandas is not installed")
            print("💡 Install with: pip install pandas")
            return
        
        async with DataQuery() as dq:
            # Example 1: Convert groups to DataFrame
            print("\n📊 Example 1: Groups to DataFrame")
            print("-" * 40)
            
                groups = await dq.list_groups_async(limit=5)
            if groups:
                groups_df = dq.to_dataframe(groups)
                print(f"✅ Groups DataFrame created:")
                print(f"   Shape: {groups_df.shape}")
                print(f"   Columns: {list(groups_df.columns)}")
                print(f"\n📋 First few rows:")
                print(groups_df.head(3).to_string())
                
                # Use specialized method
                groups_df_special = dq.groups_to_dataframe(groups)
                print(f"\n🎯 Using specialized groups_to_dataframe():")
                print(f"   Shape: {groups_df_special.shape}")
            else:
                print("❌ No groups available")
            
            # Example 2: Convert files to DataFrame
            if groups:
                print(f"\n📊 Example 2: Files to DataFrame")
                print("-" * 40)
                
                target_group = groups[0].group_id
                print(f"🎯 Using group: {target_group}")
                
                try:
                    files = await dq.list_files_async(target_group)
                    if files and hasattr(files, 'file_group_ids') and files.file_group_ids:
                        files_df = dq.files_to_dataframe(files)
                        print(f"✅ Files DataFrame created:")
                        print(f"   Shape: {files_df.shape}")
                        print(f"   Columns: {list(files_df.columns)}")
                        
                        # Show data types
                        print(f"\n📈 Data types:")
                        for col, dtype in files_df.dtypes.items():
                            print(f"   {col}: {dtype}")
                        
                        print(f"\n📋 First few rows:")
                        print(files_df.head(3).to_string())
                    else:
                        print("📭 No files found in this group")
                        
                except Exception as e:
                    print(f"⚠️  Error accessing files: {e}")
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials in .env file")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


async def advanced_dataframe_conversion_example():
    """Demonstrate advanced DataFrame conversion with custom options."""
    print("\n🔄 Advanced DataFrame Conversion Example")
    print("=" * 60)
    
    try:
        import pandas as pd
        
        async with DataQuery() as dq:
            # Get some data to work with
            groups = await dq.list_groups_async(limit=3)
            
            if not groups:
                print("❌ No groups available for advanced examples")
                return
            
            # Example 1: Custom transformations
            print("\n🔧 Example 1: Custom Transformations")
            print("-" * 50)
            
            def clean_group_id(group_id):
                """Clean group ID by removing special characters."""
                if isinstance(group_id, str):
                    return group_id.replace('-', '_').replace('.', '_').upper()
                return group_id
            
            def categorize_group(description):
                """Categorize groups based on description."""
                if not isinstance(description, str):
                    return "UNKNOWN"
                
                desc_lower = description.lower()
                if any(word in desc_lower for word in ['economic', 'gdp', 'inflation']):
                    return "ECONOMIC"
                elif any(word in desc_lower for word in ['market', 'stock', 'bond']):
                    return "MARKET"
                elif any(word in desc_lower for word in ['weather', 'climate']):
                    return "WEATHER"
                else:
                    return "OTHER"
            
            groups_df = dq.to_dataframe(
                groups,
                custom_transformations={
                    'group_id': clean_group_id,
                    'description': categorize_group
                }
            )
            
            print(f"✅ Custom transformations applied:")
            print(f"   Shape: {groups_df.shape}")
            print(groups_df[['group_id', 'description']].head(3).to_string())
            
            # Example 2: Include metadata and flatten nested
            print(f"\n📊 Example 2: Include Metadata & Flatten Nested")
            print("-" * 50)
            
            groups_detailed_df = dq.to_dataframe(
                groups,
                flatten_nested=True,
                include_metadata=True
            )
            
            print(f"✅ Detailed DataFrame with metadata:")
            print(f"   Shape: {groups_detailed_df.shape}")
            print(f"   Columns: {list(groups_detailed_df.columns)}")
            
            # Show metadata columns
            metadata_cols = [col for col in groups_detailed_df.columns if col.startswith('_')]
            if metadata_cols:
                print(f"   Metadata columns: {metadata_cols}")
            
            # Example 3: Date and numeric conversions
            print(f"\n📅 Example 3: Date and Numeric Conversions")
            print("-" * 50)
            
            # Find a group with files to demonstrate file metadata conversion
            target_group = None
            target_files = None
            
            for group in groups:
                try:
                    files = await dq.list_files_async(group.group_id)
                    if files:
                        target_group = group.group_id
                        target_files = files
                        break
                except:
                    continue
            
            if target_files:
                files_df = dq.to_dataframe(
                    target_files,
                    date_columns=['last_modified', 'created_date'],
                    numeric_columns=['file_size']
                )
                
                print(f"✅ Files with date/numeric conversions:")
                print(f"   Shape: {files_df.shape}")
                
                # Show data types after conversion
                print(f"\n📈 Converted data types:")
                for col, dtype in files_df.dtypes.items():
                    print(f"   {col}: {dtype}")
                
                # Show files with size information
                if 'file_size' in files_df.columns:
                    size_stats = files_df['file_size'].describe()
                    print(f"\n📏 File size statistics:")
                    print(f"   Count: {size_stats['count']}")
                    if size_stats['count'] > 0:
                        print(f"   Mean: {size_stats['mean']:.0f} bytes")
                        print(f"   Max: {size_stats['max']:.0f} bytes")
            
    except ImportError:
        print("❌ pandas is required for advanced examples")
    except Exception as e:
        print(f"❌ Error in advanced examples: {e}")


async def specialized_conversion_methods_example():
    """Demonstrate specialized conversion methods."""
    print("\n🔄 Specialized Conversion Methods Example")
    print("=" * 60)
    
    try:
        import pandas as pd
        
        async with DataQuery() as dq:
            # Get sample data
            groups = await dq.list_groups_async(limit=2)
            
            if not groups:
                print("❌ No groups available")
                return
            
            # Example 1: Groups specialized conversion
            print("\n🏢 Example 1: groups_to_dataframe()")
            print("-" * 40)
            
            groups_df = dq.groups_to_dataframe(groups, include_metadata=False)
            print(f"✅ Groups DataFrame (specialized method):")
            print(f"   Shape: {groups_df.shape}")
            print(f"   Columns: {list(groups_df.columns)}")
            
            # Example 2: Files specialized conversion
            print(f"\n📁 Example 2: files_to_dataframe()")
            print("-" * 40)
            
            target_group = groups[0].group_id
            try:
                files = await dq.list_files_async(target_group)
                if files and hasattr(files, 'file_group_ids') and files.file_group_ids:
                    files_df = dq.files_to_dataframe(files, include_metadata=False)
                    print(f"✅ Files DataFrame (specialized method):")
                    print(f"   Shape: {files_df.shape}")
                    print(f"   Columns: {list(files_df.columns)}")
                    
                    # Show automatic data type detection
                    print(f"\n🔍 Automatic data type detection:")
                    for col, dtype in files_df.dtypes.items():
                        if 'date' in col.lower() or 'time' in col.lower():
                            print(f"   📅 {col}: {dtype} (auto-detected as date)")
                        elif 'size' in col.lower() or 'count' in col.lower():
                            print(f"   🔢 {col}: {dtype} (auto-detected as numeric)")
                        else:
                            print(f"   📝 {col}: {dtype}")
                else:
                    print("📭 No files found for specialized conversion example")
            except Exception as e:
                print(f"⚠️  Error in files example: {e}")
            
            # Example 3: Demonstrate with instruments (if available)
            print(f"\n🏭 Example 3: instruments_to_dataframe()")
            print("-" * 40)
            
            try:
                instruments = await dq.list_instruments_async(target_group, limit=5)
                if instruments and hasattr(instruments, 'instruments') and instruments.instruments:
                    instruments_df = dq.instruments_to_dataframe(instruments)
                    print(f"✅ Instruments DataFrame:")
                    print(f"   Shape: {instruments_df.shape}")
                    print(f"   Columns: {list(instruments_df.columns)}")
                else:
                    print("📭 No instruments found or not accessible")
            except Exception as e:
                print(f"⚠️  Instruments not available: {e}")
            
    except ImportError:
        print("❌ pandas is required for specialized conversion examples")
    except Exception as e:
        print(f"❌ Error in specialized examples: {e}")


async def data_analysis_workflow_example():
    """Demonstrate a complete data analysis workflow using DataFrames."""
    print("\n🔄 Data Analysis Workflow Example")
    print("=" * 60)
    
    try:
        import pandas as pd
        
        async with DataQuery() as dq:
            # Step 1: Get groups and analyze
            print("\n📊 Step 1: Group Analysis")
            print("-" * 30)
            
            groups = await dq.list_groups_async(limit=10)
            if groups:
                groups_df = dq.groups_to_dataframe(groups)
                
                print(f"📈 Group statistics:")
                print(f"   Total groups: {len(groups_df)}")
                
                if 'group_id' in groups_df.columns:
                    # Analyze group ID patterns
                    groups_df['group_length'] = groups_df['group_id'].str.len()
                    avg_length = groups_df['group_length'].mean()
                    print(f"   Average group ID length: {avg_length:.1f} characters")
                
                if 'description' in groups_df.columns:
                    # Count groups with descriptions
                    has_desc = groups_df['description'].notna().sum()
                    print(f"   Groups with descriptions: {has_desc}/{len(groups_df)}")
            
            # Step 2: File analysis across groups
            print(f"\n📁 Step 2: File Analysis Across Groups")
            print("-" * 40)
            
            all_files_data = []
            group_file_counts = {}
            
            for group in groups[:3]:  # Analyze first 3 groups
                try:
                    files = await dq.list_files_async(group.group_id)
                    if files and hasattr(files, 'file_group_ids') and files.file_group_ids:
                        files_df = dq.files_to_dataframe(files)
                        files_df['source_group'] = group.group_id
                        all_files_data.append(files_df)
                        group_file_counts[group.group_id] = len(files_df)
                    else:
                        group_file_counts[group.group_id] = 0
                except Exception:
                    group_file_counts[group.group_id] = 0
            
            # Combine all file data
            if all_files_data:
                combined_files_df = pd.concat(all_files_data, ignore_index=True)
                
                print(f"📊 Combined file analysis:")
                print(f"   Total files across groups: {len(combined_files_df)}")
                
                # Group analysis
                group_stats = combined_files_df['source_group'].value_counts()
                print(f"   Files per group:")
                for group_id, count in group_stats.items():
                    print(f"     {group_id}: {count} files")
                
                # File size analysis (if available)
                if 'file_size' in combined_files_df.columns:
                    size_stats = combined_files_df['file_size'].dropna()
                    if len(size_stats) > 0:
                        total_size_mb = size_stats.sum() / (1024 * 1024)
                        avg_size_mb = size_stats.mean() / (1024 * 1024)
                        print(f"   Total data size: {total_size_mb:.2f} MB")
                        print(f"   Average file size: {avg_size_mb:.2f} MB")
                
                # Filename analysis
                if 'filename' in combined_files_df.columns:
                    filenames = combined_files_df['filename'].dropna()
                    if len(filenames) > 0:
                        # File type distribution
                        extensions = filenames.str.extract(r'\.([^.]+)$')[0].value_counts()
                        print(f"   File type distribution:")
                        for ext, count in extensions.head(5).items():
                            print(f"     .{ext}: {count} files")
            else:
                print("📭 No files found in accessible groups")
            
            # Step 3: Export results
            print(f"\n💾 Step 3: Export Results")
            print("-" * 30)
            
            if groups:
                # Save groups analysis
                output_dir = Path("./analysis_output")
                output_dir.mkdir(exist_ok=True)
                
                groups_df.to_csv(output_dir / "groups_analysis.csv", index=False)
                print(f"✅ Groups analysis saved to: {output_dir / 'groups_analysis.csv'}")
                
                if all_files_data:
                    combined_files_df.to_csv(output_dir / "files_analysis.csv", index=False)
                    print(f"✅ Files analysis saved to: {output_dir / 'files_analysis.csv'}")
                
                print(f"\n📊 Analysis Summary:")
                print(f"   Groups analyzed: {len(groups_df)}")
                print(f"   Files analyzed: {len(combined_files_df) if all_files_data else 0}")
                print(f"   Output directory: {output_dir.absolute()}")
            
    except ImportError:
        print("❌ pandas is required for data analysis workflow")
    except Exception as e:
        print(f"❌ Error in analysis workflow: {e}")


def show_dataframe_utilities():
    """Show utility information about DataFrame conversion."""
    print("\n🔧 DataFrame Conversion Utilities")
    print("=" * 50)
    
    print("📊 Available conversion methods:")
    print("   • to_dataframe() - General purpose converter")
    print("   • groups_to_dataframe() - Specialized for groups")
    print("   • files_to_dataframe() - Specialized for files")
    print("   • instruments_to_dataframe() - Specialized for instruments")
    print("   • time_series_to_dataframe() - Specialized for time series")
    
    print(f"\n⚙️  Conversion options:")
    print("   • flatten_nested: Flatten nested objects into columns")
    print("   • include_metadata: Include private/metadata fields")
    print("   • date_columns: Specify columns to parse as dates")
    print("   • numeric_columns: Specify columns to convert to numeric")
    print("   • custom_transformations: Apply custom functions to columns")
    
    print(f"\n🔍 Automatic detection:")
    print("   • Date columns: 'date', 'time', 'created', 'updated', 'modified', 'expires'")
    print("   • Numeric columns: 'size', 'count', 'bytes', 'price', 'value', 'volume', 'id'")
    
    print(f"\n📦 Requirements:")
    print("   • pandas: pip install pandas")
    print("   • Handles Pydantic models, dictionaries, and primitive types")
    print("   • Automatic type conversion and error handling")


async def main():
    """Run all DataFrame conversion examples."""
    print("🚀 PyDataQuery SDK - DataFrame Conversion Example")
    print("=" * 70)
    
    # Check pandas availability
    try:
        import pandas as pd
        print(f"✅ pandas {pd.__version__} is available")
    except ImportError:
        print("❌ pandas is not installed")
        print("💡 Install with: pip install pandas")
        print("🔄 Continuing with basic examples...")
    
    # Run examples
    await basic_dataframe_conversion_example()
    await advanced_dataframe_conversion_example()
    await specialized_conversion_methods_example()
    await data_analysis_workflow_example()
    
    # Show utilities information
    show_dataframe_utilities()
    
    print("\n✨ DataFrame conversion examples completed!")
    print("💡 Check the 'analysis_output' directory for exported CSV files.")
    print("💡 Use these methods to integrate DataQuery data with pandas workflows.")


if __name__ == "__main__":
    asyncio.run(main())
