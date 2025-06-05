import json
from pathlib import Path
import re


def filter_by_whitelist(data: list | dict[str, any], white_list: list[str]) -> list | dict[str, any]:
    """Filter a dataframe or dictionary based on the white_list in the config."""
    
    if not isinstance(data, list) and not isinstance(data, dict):
        raise ValueError("data must be a list or dictionary")


    if isinstance(data, list):
        # keep only columns that are in the white_list if all whitelist cols are in the dataframe
        if all(col in data.columns for col in white_list):
            return data[white_list]  
        else:
            raise ValueError("Not all whitelist columns are in the dataframe")
    
    else:  # keep only keys that are in the white_list
        if all(key in data["data"] for key in white_list) and \
            all(key in data["metadata"] for key in white_list):

            data_subset = {key: data["data"][key] for key in white_list}
            metadata_subset = {key: data["metadata"][key] for key in white_list}

            # Remerge the metadata and data subsets
            return {"data": data_subset, "metadata": metadata_subset}
            
        else:
            metadata_missing = [key for key in white_list if key not in data["metadata"]]
            data_missing = [key for key in white_list if key not in data["data"]]
            print("Not all whitelist columns are in the dataframe")
            print(f"Metadata missing: {metadata_missing}")
            print(f"Data missing: {data_missing}")
            raise ValueError(f"Not all whitelist columns are in the Codebook: {data_missing}")


def export_to_json(data: dict[str, any], folder: Path, filename: str) -> None:
        """Needed for merging  inspection - pre and post transform."""
        
        if not isinstance(filename, str):
            raise TypeError("filename must be a string")
        if not isinstance(folder, Path):
            raise TypeError("folder must be a Path object")
        if not isinstance(data, dict):
            raise TypeError("data must be a dictionary")

        output_path = folder / f"{filename}.json"
        
        json_output = json.dumps(data, indent=4, ensure_ascii=False)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_output)



def sort_whitelist(white_list: list[str]) -> list[str]:
    """Sort the whitelist to ensure consistent order.
    
    This function sorts elements both alphabetically and by their numerical components.
    For example, elements like 'A01', 'A02', 'A10' will be sorted correctly (not as 'A01', 'A10', 'A02').
    
    Parameters
    ----------
    white_list : list[str]
        List of strings to be sorted
        
    Returns
    -------
    list[str]
        Sorted list of strings
    """
    
    def natural_sort_key(s):
        """Key function for natural sorting that handles alphanumeric patterns."""
        # Split the string into text and numeric parts
        return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', s)]
    
    return sorted(white_list, key=natural_sort_key)



def merge_dicts(dict_0: dict[str, any], dict_1: dict[str, any], subkey_tag: str) -> dict[str, any]:
    
    if not isinstance(dict_0, dict):
        raise TypeError(f"dict_0 must be a dictionary, but is {type(dict_0)}")
    if not isinstance(dict_1, dict):
        raise TypeError(f"dict_1 must be a dictionary, but is {type(dict_1)}")

    # Check that both inputs have exactly the same keys
    keys_dict_0 = set(dict_0.keys())
    keys_dict_1 = set(dict_1.keys())


    # The key changes for append_column edit are:
    # 
    # - Changed the validation to only check if dict_1 is missing keys from dict_0 (not requiring identical keys)
    # - Added a second loop to handle any additional keys in dict_1 that weren't in dict_0
    # - For these additional keys, we create a new entry in the merged dictionary with the subkey_tag structure


    missing_keys = keys_dict_0 - keys_dict_1
    if missing_keys:
        raise ValueError(f"dict_1 is missing keys that are in dict_0: {missing_keys}")
    
    additional_keys = keys_dict_1 - keys_dict_0
    if additional_keys:
        print(f"dict_1 has additional keys that are not in dict_0: {additional_keys}")
        print("These are likely due to append_column function")
     
    # The first dictonary already contains the keys with subdictionaries
    # We will add the content of each key in the second dictionary as
    # a subdictionary to each key in the first dictionary, 
    # given the subkey string like : "_PROCESSED"

    # 3 Layer Interface Model
    # TUI, CLI+JSON, 

    merged = {}

    for key in dict_0.keys():
        #print(dict_0, "\n\n")
        #print(dict_1, "\n\n")
        merged[key] = dict_0[key]
        merged[key][subkey_tag] = dict_1[key]


    # Add any additional keys from dict_1 that weren't in dict_0
    for key in additional_keys:
        merged[key] = {subkey_tag: dict_1[key]}

    return merged



#  --------- LEGACY FUNCTIONS ---------

def merge_length_maps(dict_0: dict[str, any], dict_1: dict[str, any], nest: bool = False) -> dict[str, any]:
    """
    Merges two key length maps produced by extract_keylen_map().

    Parameters
    ----------
    dict_0 : dict[str, Any] (should be the dict containing value lengths)
        Key length map containing value lengths, output from extract_length_map()
    dict_1 : dict[str, Any] (should be the dict containing key lengths)
        Key length map containing key lengths, output from extract_length_map()

    nest : bool, default = False
        If true, we are probably adding metadata to codebook_data["metadata"], so
        the metadata dicts are concatenated to key_lengths. 
    Returns
    -------
    dict[str, Any]
 
    """
    merged = {}

    # Check that both inputs have exactly the same keys
    keys_dict_0 = set(dict_0.keys())
    keys_dict_1 = set(dict_1.keys())
    
    if keys_dict_0 != keys_dict_1:
        raise ValueError(f"Input maps must have identical keys. Found differing keys:\n"
                        f"Only in dict_0: {keys_dict_0 - keys_dict_1}\n"
                        f"Only in dict_1: {keys_dict_1 - keys_dict_0}")
    
    # Process each column/key that appears in either input
    
    for key in keys_dict_0:
        if not nest:
            merged[key] = {
                "sensitive_lengths": dict_0.get(key, {}),
                "codebook_lengths": dict_1.get(key, {})
            }
        else:
            combined_lens = {"key_lengths": dict_1.get(key, {}), **dict_0.get(key, {})}
            merged[key] = combined_lens
    return merged

def plot_length_distribution(length_map: dict[str, any], title: str, path: Path) -> None:
    """Plot two charts showing the distribution of key lengths:
    1. Stacked bar chart with log scale for absolute values
    2. Stacked bar chart with percentage distribution (wider for better readability)
    
    Args:
        length_map: Dictionary output from extract_keylen_map
        title: Title for the plot
        path: Path object where the plot will be saved
    """

    assert isinstance(length_map, dict), "length_map must be a dictionary"
    assert isinstance(title, str), "title must be a string"
    assert isinstance(path, Path), "path must be a Path object"


    # Convert the nested dict to DataFrame
    df_data = []
    output_path = path / f"{title}.png"

    for key, length_counts in length_map.items():
        if "null" in length_counts:
            df_data.append({
                'ItemKey': key,
                'KeyLength': -1,
                'Count': 0
            })
        else:
            for length, count in length_counts.items():
                df_data.append({
                'ItemKey': key,
                'KeyLength': length,
                'Count': count
            })
    
    plot_df = "xxx"
    plot_df = plot_df.pivot(index='ItemKey', columns='KeyLength', values='Count')
    plot_df = plot_df.fillna(0)
    
    # Calculate percentage distribution
    plot_df_pct = plot_df.div(plot_df.sum(axis=1), axis=0) * 100
    
    # Dynamic figure sizing based on number of items
    num_items = len(plot_df)
    base_width = 12
    base_height = 8
    
    if num_items > 20:
        width = base_width + (num_items - 20) * 0.25
        width = min(width, 36)
    else:
        width = base_width
    
    # Create figure with two subplots of different widths (1:2 ratio)
    fig = plt.figure(figsize=(width * 2.2, base_height))
    gs = plt.GridSpec(1, 2, width_ratios=[1, 2])
    
    # Plot 1: Stacked bars with log scale (smaller)
    ax1 = fig.add_subplot(gs[0])
    plot_df.plot(kind='bar', stacked=True, ax=ax1, legend=False)
    ax1.set_yscale('log')
    ax1.set_ylabel('Count of Keys (Log Scale)')
    ax1.set_xlabel('ItemKey')
    ax1.set_title('Focus on Number of Keys')
    ax1.tick_params(axis='x', rotation=90)
    
    # Plot 2: Percentage distribution (wider)
    ax2 = fig.add_subplot(gs[1])
    plot_df_pct.plot(kind='bar', stacked=True, ax=ax2, legend=False)
    ax2.set_ylabel('Percentage of Keys')
    ax2.set_xlabel('ItemKey')
    ax2.set_title('Focus on Key Lengths')
    ax2.tick_params(axis='x', rotation=90)
    
    # Add percentage labels for all segments except those with 0%
    for c in ax2.containers:
        # Add labels with different formats based on percentage value
        labels = [f'{v:.2f}%' if v < 1 else f'{v:.2f}%' for v in c.datavalues]
        # Filter out 0% labels
        labels = [label if float(label[:-1]) > 0 else '' for label in labels]
        ax2.bar_label(c, 
                     labels=labels,
                     label_type='center',
                     rotation=0,
                     padding=5,
                     fontsize=6)
    
    # Adjust legend position and format based on plot width
    if width > 24:
        plt.figlegend(title='KeyLength', 
                     bbox_to_anchor=(0.5, -0.1),
                     loc='upper center', 
                     ncol=min(8, len(plot_df.columns)),
                     borderaxespad=0)
        plt.tight_layout(rect=[0, 0.1, 1, 0.95])
    else:
        plt.figlegend(title='KeyLength',
                     bbox_to_anchor=(1.05, 1),
                     loc='upper left')
        plt.tight_layout()
    
    # Add overall title
    fig.suptitle(title, y=1.02)
    
    plt.savefig(output_path, 
                bbox_inches='tight',
                dpi=300,
                format='png')
    plt.close()