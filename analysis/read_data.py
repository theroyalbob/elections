def read_file_content(response, filename):
    """Read file content based on file extension and apply column names"""
    try:
        # Determine if this is a registration or returns file
        is_registration = 'registration' in filename.lower() or 'vrstat' in filename.lower()
        columns = registration_columns if is_registration else returns_columns
        
        if filename.endswith('.xlsx'):
            df = pd.read_excel(BytesIO(response.content), header=None)
            df.columns = columns
            
        elif filename.endswith('.csv'):
            # Try different delimiters
            for delimiter in [',', '\t', '|', ';']:
                try:
                    df = pd.read_csv(StringIO(response.text), delimiter=delimiter, header=None)
                    df.columns = columns
                    break
                except:
                    continue
            else:
                raise ValueError(f"Could not determine delimiter for {filename}")
                
        elif filename.endswith('.txt'):
            # Try different delimiters for txt files
            for delimiter in [',', '\t', '|', ';']:
                try:
                    df = pd.read_csv(StringIO(response.text), delimiter=delimiter, header=None)
                    df.columns = columns
                    break
                except:
                    continue
            else:
                # If all delimiter attempts fail, try fixed width
                df = pd.read_fwf(StringIO(response.text), header=None)
                df.columns = columns

        # Apply dictionary mappings
        if df is not None:
            df = process_codes(df, is_registration)
            
        return df

    except Exception as e:
        print(f"Error reading {filename}: {str(e)}")
        return None 