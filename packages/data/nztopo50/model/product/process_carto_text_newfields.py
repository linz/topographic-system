import pandas as pd



def read_and_process_layers(mapping_spreadsheet, full_layers_sheet):
    """
    Read the Excel file and process 'Full layers' sheet.
    Skip fully blank lines and process sections starting with 'ATT' in layer column.
    """
    try:
        # Read the Excel file and specific sheet
        df = pd.read_excel(mapping_spreadsheet, sheet_name=full_layers_sheet)
        
        # Remove completely empty rows (all NaN values)
        df = df.dropna(how='all')
        
        # Find the column that contains layer information (assuming it's the first column or named 'layer')
        layer_column = df.columns[0]  # Assuming layer info is in first column
        
        # Process ATT sections
        att_sections = []
        current_section = []
        current_att_name = None
        
        for index, row in df.iterrows():
            layer_value = str(row[layer_column]).strip() if pd.notna(row[layer_column]) else ""
            
            # Check if this row starts a new ATT section
            if layer_value.startswith('ATT'):
                # Save previous section if it exists
                if current_section:
                    att_sections.append({
                        'att_name': current_att_name,
                        'data': current_section
                    })
                
                # Start new section
                current_att_name = layer_value
                current_section = [row]
                print(f"Found ATT section: {current_att_name}")
                
            elif current_att_name:  # We're inside an ATT section
                # Add row to current section until we hit next ATT or end
                current_section.append(row)
        
        # Don't forget the last section
        if current_section:
            att_sections.append({
                'att_name': current_att_name,
                'data': current_section
            })
        
        print(f"Processed {len(att_sections)} ATT sections")
        
        # Process each ATT section
        for section in att_sections:
            process_att_section(section['att_name'], section['data'])
        
        return att_sections
        
    except FileNotFoundError:
        print(f"Error: Could not find file {mapping_spreadsheet}")
        return None
    except Exception as e:
        print(f"Error processing Excel file: {e}")
        return None

def process_att_section(att_name, section_data):
    """
    Process individual ATT section data.
    """
    print(f"\nProcessing section: {att_name}")
    print(f"Number of rows in section: {len(section_data)}")
    
    # Convert section data to DataFrame for easier processing
    section_df = pd.DataFrame(section_data)
    
    # Display the columns available for processing
    print(f"Available columns: {list(section_df.columns)}")
    
    # Process the section data here
    # You can add specific processing logic based on your requirements
    for i, row in enumerate(section_data):
        if i == 0:  # Skip the ATT header row for detail processing
            continue
        # Process each data row in the section
        # Add your specific processing logic here
        pass

if __name__ == "__main__":
    # Run the processing
    mapping_spreadsheet = r"C:\Data\Topo50\Topo50_carto_text_2020_09.xlsx"
    full_layers_sheet = 'Full layers'
    att_sections = read_and_process_layers(mapping_spreadsheet, full_layers_sheet)
    
    if att_sections:
        print(f"\nSuccessfully processed {len(att_sections)} ATT sections from {full_layers_sheet} sheet")
    else:
        print("Failed to process the Excel file")