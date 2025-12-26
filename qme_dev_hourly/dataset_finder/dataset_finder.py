import os
import yaml
import pandas as pd

from tabulate import tabulate


def extract_from_format(format_string, input_string):
    """
    Extract values from an input string according to a given format string.
    Values to be extracted should be surrounded by {} in the format string.
    Raises an error if the input string does not otherwise match the format string.
    If a variable name is "*", the matched value will be ignored instead of returned.
    This function uses the characters proceeding the end brace } to determine where
    the match ends in the input string. Therefore, if those characters themselves
    are the value intended to be matched, the strings will be considered mismatched.
    Example: If attempting to match "{a}_{b}" with a string where the value of "a" is
    intended to be "1_2", only the "1" would be matched to "a". This is usually only
    a problem if the separator between variables is a single character.
    Inputs:
    - format_string - a string containing the format, with {} around variables
    - input_string - a string following the format whose values are to be extracted
    Returns:
    A dictionary mapping the names of the variables from format_string to the values found in input_string.
    """
    extracted_values = {}
    while format_string:
        
        # no variables left
        if "{" not in format_string:
            if format_string != input_string:
                # print(format_string, input_string, extracted_values, sep, sep_pos)
                raise ValueError("Strings are not the same format")
            break
        
        # find argument start point
        arg_start = format_string.find("{")
    
        # cut out non-variable filler (for example, "v1-r1-ACS-{bc}-{ref}" will become "bc}-{ref}"
        # but first check it matches just in case the strings aren't the same format
        if format_string[:arg_start] != input_string[:arg_start]:
            raise ValueError("Strings are not the same format, values cannot be matched")
            # break
        
        format_string = format_string[arg_start + 1:]
        input_string = input_string[arg_start:]
    
        # find the next character after variable and use it to extract from check string
        arg_end = format_string.find("}")
        var_name = format_string[:arg_end]
        format_string = format_string[arg_end + 1:]

        # unspecified length
        var_length = 0
        
        # find special formatting options for var
        if ":" in var_name:
            split = var_name.split(":")
            var_name = split[0]
            var_length = int(split[1])

        # check if at end (i.e. nothing more to cut out)
        if format_string:
            if var_length:
                sep_pos = var_length

            else:
                # get the separator (multiple characters to allow for year filtering later)
                if "{" in format_string:
                    sep_end = format_string.find("{")
                    sep = format_string[:sep_end]
                else:
                    sep = format_string
                sep_pos = input_string.find(sep)
            var_value = input_string[:sep_pos]
            input_string = input_string[sep_pos:]

        else:
            var_value = input_string

        # match all - don't raise an error, but also don't keep result
        if var_name != "*":
            extracted_values[var_name] = var_value
            # if var_name not in extracted_values:
            #     extracted_values[var_name] = var_value
            # else:
            #     if isinstance(extracted_values[var_name], str):
            #         if var_value != extracted_values[var_name]:
            #             extracted_values[var_name] = [extracted_values[var_name], var_value]
            #     else:
            #         if var_value not in extracted_values[var_name]:
            #             extracted_values[var_name].append(var_value)
    
    return extracted_values


# important: this can be done in-place
def match_values(arr, format_string, search_terms, exact_match = False, in_place = True, exact_match_dict = {}):
    """
    Takes a list of strings, extracts their values according to the given format, and tries to match them against
    a given dictionary of search terms. Results that do not match are removed from the list (in place by default).
    Each given search term can be assigned multiple values - only one of them needs to match for the string to be kept.
    There is the option to match exactly or not (in which case any given search term is considered a match if it
    is a substring of the string it is being tested against). Additionally, individual terms can be designated
    to be exact match terms or not, overriding the global value.
    Matches are not case sensitive, even if exact_match is True.
    Inputs:
    - arr: A list of strings in the format given by format_string
    - format_string: The format which will be used to extract values out of array strings for matching
    - search_terms: A dictionary mapping variables names to values (can be individual or list for each value)
    - exact_match: Whether to match exactly or by substring (default False)
    - in_place: Whether to modify arr in place or to create and return a new array (default True)
    - exact_match_dict: A dictionary mapping variable names to True or False values for whether they should
    be exact matches. If a given variable name is not included, exact_match will be used instead
    Output:
    The original array of strings with any non-matching strings removed, or a new array with all matching
    strings included, depending on in_place.
    """
    # turn strings into lists with a single term so that later loops don't loop by letter
    for key in search_terms:
        if isinstance(search_terms[key], str):
            search_terms[key] = [search_terms[key]]

    to_remove = []

    for item in arr:

        try:
            extracted_values = extract_from_format(format_string, item)
    
            for key in extracted_values:
                
                check_value = extracted_values[key]
                range_check = False

                if "!" in key:
                    split = key.split("!")
                    key = split[0]

                    # todo: make more safe (check if start was there)
                    if split[1] == "end":
                        continue

                    elif split[1] == "start":
                        range_check = True
                        check_value = year_range(check_value, extracted_values[f'{key}!end'])
                
                # check if it's being searched by - if not, match by default
                if key in search_terms:
                    remove = True
                    for value in search_terms[key]:
                        if range_check:
                            if value in check_value:
                                remove = False
                        else:
                            if exact_match_dict[key] if key in exact_match_dict else exact_match:
                                if value.casefold() == check_value.casefold():
                                    remove = False
                            else:
                                if value.casefold() in check_value.casefold():
                                    remove = False
                    if remove:
                        to_remove.append(item)
                        
                # already failed against one of the search terms, can move on to next item
                if item in to_remove:
                    break

        # failed to match properly
        except:
            to_remove.append(item)

    # use string method to modify existing list
    if in_place:
        for item in to_remove:
            arr.remove(item)
        return arr

    # create new list
    else:
        new_arr = [item for item in arr if item not in to_remove]
        return new_arr


class dataset_info:
    def __init__(self, data, root, format_file):
        self.data = data
        # self.root = root
        self.roots = [root]
        self.format_file = format_file
        self.info = {}
        self.generated_info_filtered = None
        self.generated_info_unfiltered = None
        self.info_str = ""
        self.selected = {}
        self.priority = {}
        self.exact_match_dict = {}

    def __repr__(self):
        return tabulate([*self.data.items()])

    # def _repr_html_(self):
    #     return tabulate([*self.data.items()], tablefmt = "html", headers=["Key", "Value"])

    def _repr_html_(self):
        return tabulate([*self.table_data(False).items()], tablefmt = "html", headers=["Key", "Value"])

    def table_data(self, add_separator = True):
        return self.data | ({'<th style="border-left: 2px solid"></th>': '<td style="border-left: 2px solid"></td>'} if add_separator else {}) | {key: merge_values(value) for key, value in self.get_info().items()}

    def select(self, exact_match = False, **kwargs):
        for key in kwargs:
            self.selected[key] = kwargs[key]
            self.exact_match_dict[key] = exact_match
        self.refresh_info()
        # print(kwargs)
        return self

    def prioritise(self, key, preferences = [], default = None):
        if isinstance(preferences, str):
            preferences = [preferences]
        
        if key not in self.priority:
            self.priority[key] = {"default": default if default is not None else "error", "preferences": preferences}
        else:
            self.priority[key]["preferences"] = preferences
            if default is not None:
                self.priority[key]["default"] = default
        return self
            
    def deselect(self, *args):
        for key in args:
            if key in self.selected:
                self.selected.pop(key)
                self.exact_match_dict.pop(key)
        self.refresh_info()
        return self

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def any_files(self):
        for output in self.generate_info(True):
            return True
        return False

    def attempt_merge(self, other):
        # Check whether their data is identical (both keys and values match)
        # If they all match perfectly, this set should be empty (^ is XOR operator)
        if self.data.items() ^ other.data.items():
            return False

        else:
            # Combine the two by adding the other's root paths to this one
            new_roots = [root for root in other.roots if root not in self.roots]
            if new_roots:
                self.roots += new_roots
                self.refresh_info(True)
            return True

    def get_generated_info(self, apply_filter):
        if apply_filter:
            if self.generated_info_filtered is None:
                self.generated_info_filtered = [output for output in self.generate_info(True)]
            chosen = self.generated_info_filtered

        else:
            if self.generated_info_unfiltered is None:
                self.generated_info_unfiltered = [output for output in self.generate_info(False)]
            chosen = self.generated_info_unfiltered
            
        for info, file in chosen:
            yield info.copy(), file

    def refresh_info(self, unfiltered = False):
        self.generated_info_filtered = None
        if unfiltered:
            self.generated_info_unfiltered = None        
            
    def generate_info(self, apply_filter = True):

        if self.format_file[0] == os.sep:
            format_file = self.format_file[1:]
        else:
            format_file = self.format_file

        if format_file[-1] == os.sep:
            folder_mode = True
            format_file = format_file[:-1]
        else:
            folder_mode = False

        for path_root in self.roots:
            if "{" in format_file:
                
                first_arg_pos = format_file.find("{")
        
                # cut off by folder in case the variable wasn't immediately after /
                slash_pos = format_file[first_arg_pos::-1].find(os.sep)
                if slash_pos == -1:
                    prev_separator_pos = -1
                else:
                    prev_separator_pos = first_arg_pos - slash_pos
        
                start_path = path_root + format_file[:prev_separator_pos + 1]
                format_file = format_file[prev_separator_pos + 1:]
                
            else:
                start_path = path_root + format_file
                format_file = ""

            for root, dirs, file_list in os.walk(start_path, followlinks = True):
                dirs.sort()
                file_list.sort()

                # if format_file[-1] == os.sep:
                if folder_mode:
                    files = dirs
                else:
                    files = file_list

                # print(files)
                
                # how deep we are into the tree (root = 0)
                level = 0
    
                short_root = root.replace(start_path, '')
                if root != start_path:
                    level = short_root.count(os.sep) + 1
    
                if level == format_file.count(os.sep):
                    for i, file in enumerate(files):
                        files[i] = os.path.join(short_root, file)
    
                    if apply_filter and self.selected:
                        match_values(files, format_file, self.selected, in_place = True, exact_match_dict = self.exact_match_dict)

                    # print(files)
                    
                    for file in files:
                        try:
                            values = {key: value for key, value in extract_from_format(format_file, file).items() if key not in self.data}
                        except:
                            # print(format_file, file)
                            continue
                        yield values, path_root + file
    
    def collate_info(self, apply_filter = True):
        def collate_info_recursive(current_dict, info):

            if info.keys():
                key = list(info.keys())[0]
            else:
                return
                
            values = info.pop(key)

            if isinstance(values, str):
                values = [values]

            for value in values:
        
                if key in current_dict:
                    if not info:
                        current_dict[key][value] = {}
                    else:
                        if value not in current_dict[key]:
                            new_dict = {}
                            current_dict[key][value] = new_dict
                        collate_info_recursive(current_dict[key][value], info)
            
                if key not in current_dict:
                    if not info:
                        current_dict[key] = {value: {}}
                    else:
                        new_dict = {}
                        current_dict[key] = {value: new_dict}
                        collate_info_recursive(new_dict, info)

        collated_info = {}
        self.info = {}
        
        # for info, file in self.generate_info(apply_filter):
        for info, file in self.get_generated_info(apply_filter):
            to_pop = []
            for key in list(info.keys()):
                new_value = [info[key]]

                if "!" in key:
                    to_pop.append(key)
                    split = key.split("!")
                    key = split[0]

                    # todo: make more safe (check if start was there)
                    if split[1] == "end":
                        continue

                    elif split[1] == "start":
                        range_check = True
                        new_value = year_range(new_value[0], info[f'{key}!end'])
                        info[key] = new_value
                        
                if key not in self.info:
                    self.info[key] = new_value
                else:
                    for item in new_value:
                        if item not in self.info[key]:
                            self.info[key].append(item)
                            # self.info[key] = self.info[key] + new_value
                    self.info[key].sort()
            for pop_key in to_pop:
                info.pop(pop_key)
            collate_info_recursive(collated_info, info)
        return collated_info

    def get_info(self, apply_filter = True):
        self.collate_info(apply_filter)
        
        # if not self.info:
        #     self.collate_info()
    
        return self.info

    def print_info(self):
        if self.info_str:
            print(self.info_str)
            return
            
        def print_info_recursive(current_info, depth = 0):
            if not current_info:
                return ""
            var_name, info = list(current_info.items())[0]
            keys = list(info.keys())
            out_str = ""
            while keys:
                key = keys[0]
                merged_keys = [key]
                value = info[key]
                for next_key in keys[1:]:
                    if info[next_key] == value:
                        merged_keys.append(next_key)

                out_str = out_str + "   " * depth + f"{var_name}: {merge_values(merged_keys)}"
                if value:
                    out_str = out_str + "\n" + print_info_recursive(value, depth + 1)

                for item in merged_keys:
                    keys.remove(item)

                if keys:
                    out_str = out_str + "\n\n"

            return out_str
        
        collated_info = self.collate_info()
        self.info_str = print_info_recursive(collated_info)
        print(self.info_str)
        # return self.info_str
    
    # def match(self, key, match_terms, exact_match = False):
    #     if isinstance(match_terms, str):
    #         match_terms = [match_terms]
    #     if exact_match:
    #         return any(term == self.data[key] for term in match_terms)
    #     else:
    #         return any(term in self.data[key] for term in match_terms)
    
    def match(self, exact_match = False, **search_terms):

        # currently_matching = False
        
        for key in search_terms:
            if isinstance(search_terms[key], str):
                search_terms[key] = [search_terms[key]]

            if key not in self.data:
                return False

            ## FIX THIS
            if exact_match:
                if not any(term == self.data[key] for term in search_terms[key]):
                    return False
            else:
                if not any(term in self.data[key] for term in search_terms[key]):
                    return False

        return True


    def includes(self, exact_match = False, **kwargs):
        """
        Checks whether given keyword argument search terms are contained with the dataset.
        """
        for key in kwargs:
            
            if key not in self.get_info():
                return False
                
            values = kwargs[key]
            if isinstance(values, str):
                values = [values]
                
            for value in values:
                if exact_match:
                    if not any(value == term for term in self.get_info()[key]):
                        return False
                else:
                    if not any(value in term for term in self.get_info()[key]):
                        return False
                        
        return True

    def __iter__(self):
        """
        Returns the file list as an iterator so this object can be passed directly into open_mfdataset without needing to call get_files on it.
        """
        return iter(self.get_files())

    def get_files(self):
        """
        Get files for loading the dataset according to current selection, and attempt to resolve any clashes for unique terms.
        """

        # WIP
        current_files = []
        clashes = {}
        # for new_info, new_file in self.generate_info(True):
        for new_info, new_file in self.get_generated_info(True):
            to_append = True
            for old_info, old_file in current_files:
                unmatching_keys = [key for key in new_info.keys() if new_info[key] != old_info[key]]

                # identical entry detected
                if not unmatching_keys:
                    # print("identical entry warning")
                    to_append = False
                    break

                # non-identical entry with clash on non-priority key (such as year)
                if any(key not in self.priority for key in unmatching_keys):
                    continue

                # non-identical entry with clash only on priority key(s) (such as date_created)
                else:
                    # if len(unmatching_keys) > 1:
                    #     print("uh oh, multiple keys")
                    prev_to_append = None
                    for key in unmatching_keys:
                        # key = unmatching_keys[0]
                        old_value = old_info[key]
                        new_value = new_info[key]
    
                        preferences = self.priority[key]["preferences"]
                        default = self.priority[key]["default"]
                        
                        # old value in preferences list
                        if old_value in preferences:
    
                            # new value is not in preferences, does not replace previous
                            if new_value not in preferences:
                                new_to_append = False
    
                            # new value is in preferences - compare positions in preferences array
                            else:
                                new_to_append = preferences.index(new_value) < preferences.index(old_value)
    
                        # old value not in preferences list
                        else:
    
                            # new value is in preferences, replaces previous
                            if new_value in preferences:
                                new_to_append = True
    
                            # new value is not in preferences, compare according to preference
                            else:
                                if default == "high":
                                    new_to_append = new_value > old_value
                                elif default == "low":
                                    new_to_append = new_value < old_value
                                elif default == "error":
                                    raise ValueError(f'Unresolved clash between {old_value} and {new_value} for {key} - please select one with ".select({key} = ...)"')
                                else:
                                    raise ValueError(f'Unknown value for "default" parameter for {key}')
    
                                
                                # XOR statement - if default is True, it flips the result of the comparison (which is checking if old_value is greater)
                                # to_append = default ^ (old_value > new_value)

                        if prev_to_append is None:
                            prev_to_append = new_to_append
                        elif prev_to_append != new_to_append:
                            raise ValueError("Clashes caused competing outcomes - please resolve by selecting more specifically")

                    to_append = prev_to_append
                    
                    if to_append:
                        current_files.remove((old_info, old_file))

                    if key not in clashes:
                        clashes[key] = {}
                        
                    clash_details = '"' + '" over "'.join([new_value, old_value] if to_append else [old_value, new_value]) + '"'
                    # clash_details = ' over '.join([new_value, old_value] if to_append else [old_value, new_value])
                    # clash_details = [new_value, old_value] if to_append else [old_value, new_value]

                    clash_info = {info_key: new_info[info_key] for info_key in new_info.keys() if info_key != key} 
                    
                    if clash_details not in clashes[key]:
                        clashes[key][clash_details] = {}
                    else:
                        for info_key in list(clash_info.keys()):

                            info_value = [clash_info[info_key]]
                
                            if "!" in info_key:
                                split = info_key.split("!")
                                info_key = split[0]
            
                                # todo: make more safe (check if start was there)
                                if split[1] == "end":
                                    continue
            
                                elif split[1] == "start":
                                    range_check = True
                                    info_value = year_range(info_value[0], clash_info[f'{info_key}!end'])

                            if info_key not in clashes[key][clash_details]:
                                clashes[key][clash_details][info_key] = info_value
                            else:
                                clashes[key][clash_details][info_key] += [add_value for add_value in info_value if add_value not in clashes[key][clash_details][info_key]]
                    
                    break
                    
            if to_append:
                current_files.append((new_info, new_file))
            
            # pass

        for clash_key, clash_dict in clashes.items():
            for clash_details in clash_dict.keys():
                print(f'INFO: Clash on {clash_key}: Chose {clash_details} for ' + "; ".join([f'{key} = {merge_values(value)}' for key, value in clash_dict[clash_details].items()]))

        return [(file).replace(2 * os.sep, os.sep) for (info, file) in current_files]

        
        # return [(self.root + file).replace(2 * os.sep, os.sep) for (info, file) in self.generate_info(True)]
        # return [(file).replace(2 * os.sep, os.sep) for (info, file) in self.generate_info(True)]

    def to_df_table(self):
        """
        UNTESTED WITH RECENT CHANGES, AVOID USING
        """
        print("Warning - untested after flexibility changes. Use at risk of inaccuracy")
        info = self.get_info().items()
        tabled_info = {}
        # untabled_info = []
        for key, value in info:
            if len(value) == 1:
                tabled_info[key] = value[0]
            # else:
            #     untabled_info.append(key)

        if "{" in self.format_file:
            split_format_file = self.format_file.split("{")
            final_format_file = ""
            for item in split_format_file:
                # not empty string or first entry
                if "}" in item:
                    # add back in the { that was lost during split
                    item = "{" + item
                    try:
                        formatted_item = (item).format(**(self.data | tabled_info))
                    except:
                        formatted_item = item
                    final_format_file += formatted_item
                else:
                    final_format_file += item
        return (self.data | tabled_info) | {"format_file" : (self.roots[0] + final_format_file).replace(2 * os.sep, os.sep)}


class dataset_info_collection:
    def __init__(self, items = []):
        if items:
            self.items = items
        else:
            self.items = []

    def add(self, item):
        """
        Add a new dataset_info object to the collection.
        """
        self.items.append(item)

    def get_all(self, key):
        """
        Get every unique entry for a given key.
        """
        all_list = []
        for item in self.items:
            if key in item.data:
                value = item.data[key]
                if value not in all_list:
                    all_list.append(value)
        return all_list

    def prioritise(self, key, preferences = [], default = None):
        for item in self.items:
            item.prioritise(key, preferences, default)

    # select variables within the dataset to include
    def select(self, exact_match = False, remove_empty = True, **kwargs):
        """
        Select terms within every dataset of the collection. By default, return a new dataset_info_collection containing
        only non-empty entries after selection has been applied.
        """
        # use "includes" first to filter out listings which don't have required arguments
        if remove_empty:
            # returns here to do selection after "includes"
            return self.includes(exact_match, **kwargs).select(exact_match, remove_empty = False, **kwargs)
            
        for item in self.items:
            item.select(exact_match, **kwargs)
            
        return self

    # deselect variables
    # warning - if this was used after select was called with "remove_empty", it will not recover the removed items
    def deselect(self, *args):
        for item in self.items:
            item.deselect(*args)
        return self

    # return all the files from all the dataset_info objects in a single 1D list
    def get_files(self):
        """
        Return files from every dataset contained within the collection.
        Warning that only in certain circumstances will this be practical to use without having clashing coordinates once datasets are loaded into memory. 
        """
        files = []
        for item in self.items:
            files = files + item.get_files()
        return files

    def filter(self, exact_match = False, **kwargs):
        """
        Return a new dataset_info_collection containing only entries that match the given keyword argument search terms.
        """
        return dataset_info_collection([item for item in self.items if item.match(exact_match, **kwargs)])

    def _compare_collections(self, other, match_keys, exclude_keys):
        matched = []
        unmatched = []

        if match_keys is not None and exclude_keys is not None:
            raise ValueError("Can specify either match_keys or exclude_keys but not both")

        if isinstance(other, dataset_info):
            other = dataset_info_collection([other])

        if isinstance(match_keys, str):
            match_keys = [match_keys]

        if isinstance(exclude_keys, str):
            exclude_keys = [exclude_keys]
        
        for item in self.items:
            success = False
            
            for check in other.items:
                if match_keys:
                    common_keys = match_keys

                    # item does not contain all the requisite keys for comparing
                    if any([key not in item.keys() for key in common_keys]):
                        break

                    # check does not contain all the requisite keys for comparing
                    if any([key not in check.keys() for key in common_keys]):
                        break
                        
                else:
                    # match based on common columns
                    common_keys = item.keys() & check.keys()

                    # remove any specified keys
                    if exclude_keys:
                        common_keys = common_keys ^ set(exclude_keys)

                    # no common column to match between
                    if not common_keys:
                        break

                # check for match
                success = all([item.data[key] == check.data[key] for key in common_keys])

                # successful match, end early
                if success:
                    break

            if success:
                matched.append(item)
            else:
                unmatched.append(item)

        return matched, unmatched

    def find_matches(self, other, match_keys = None, exclude_keys = None):
        matched, unmatched = self._compare_collections(other, match_keys, exclude_keys)
        return dataset_info_collection(matched)

    def find_missing(self, other, match_keys = None, exclude_keys = None):
        matched, unmatched = self._compare_collections(other, match_keys, exclude_keys)
        return dataset_info_collection(unmatched)

    def to_dataframe(self):
        # return pd.DataFrame([item.data for item in self.items])
        # return pd.DataFrame([item.table_data() for item in self.items])
        return pd.DataFrame([item.to_df_table() for item in self.items])

    def includes(self, exact_match = False, **kwargs):
        return dataset_info_collection([item for item in self.items if item.includes(exact_match, **kwargs)])

    def condense(self, column, force_unique = True):
        new_collection = dataset_info_collection()
        for item in self.items:
            if column in item.data:
                item.data.pop(column)
                item.refresh_info()
                if force_unique:
                    if column in item.get_info():
                        item.prioritise(column)
            if item.any_files():
                for new_item in new_collection.items:
                    if new_item.attempt_merge(item):
                        break
                else:
                    new_collection.add(item)
        return new_collection

    # ~~so that open_mfdataset can be used directly with this object~~
    def __iter__(self):
        return iter(self.items)
        # return iter(self.get_files())

    def __add__(self, other):
        return dataset_info_collection(self.items + other.items)

    def __len__(self):
        return len(self.items)

    # allows individual dataset_info objects to be extracted directly
    # needs improvement, a slice should return a new dataset_info_collection
    # containing all the objects instead of a list (to preserve class methods)
    def __getitem__(self, key):
        return self.items[key]

    def __repr__(self):
        return tabulate([item.data for item in self.items], headers = "keys", showindex = True)

    # display nicely when interrogating on JupyterLab
    def _repr_html_(self):
        # return tabulate([item.data | {key: merge_values(value) for key, value in item.get_info().items()} for item in self.items], headers = "keys", showindex = True, tablefmt = "html")
        return tabulate([item.table_data() for item in self.items], headers = "keys", showindex = True, tablefmt = "unsafehtml")


def filter_all(format_dirs_list, format_files_list, unique = None, exact_match = False, **kwargs):
    """
    Search through a directory and its subdirectories, filtering out results that do not match
    according to the given format strings and supplied variables, returning a list of applicable datasets.
    Here "dataset" is defined slightly arbitrarily and may need clarification.
    Essentially, format_dirs + format_file should compose the entire path of any single data file,
    but format_dirs is where the filter function will stop, with any subdirectories in format_file
    considered to be a part of the dataset. This could include different weather variables so that
    they are grouped together instead of being listed as separate datasets.
    For example:
    If a file's format is /a/b/c/{x}/{y}/{z}/{var}/{year}.nc, format_dirs could be "/a/b/c/{x}/{y}/{z}/"
    and format_file could be /{var}/{year}.nc. An individual returned result would be an "{x} {y} {z}"
    dataset, with any variables it contains being considered a part of it.
    For simplicity this could simply encapsulate every subdirectory. If this is the case it is still
    important to keep the file format itself separate as only directories are checked in this function.
    Input:
    - format_dirs_list: A list of the formats of the directories leading to the datasets
    - format_file_list: A list of the formats of the files within the datasets, which can include subdirectories
    - exact_match: Whether to match search terms exactly, default to False. Otherwise a substring 
    is considered a match
    - unique: A dictionary of keys within datasets that should be unique, and the properties for resolving clashes
    - **kwargs: Keyword arguments mapping search terms to values for matching. Multiple values can
    be assigned to each search term - only one needs to match for it to be included.
    Output:
    A dataset_info_collection object containing a list of dataset_info objects corresponding to
    successful matches.
    """

    if isinstance(format_dirs_list, str):
        format_dirs_list = [format_dirs_list]

    if isinstance(format_files_list, str):
        format_files_list = [format_files_list]

    # internal helper function, can't be used from outside
    # walk through directory tree to find datasets, filtering by matching names against columns along the way
    def filter_walk(start_path, columns, exact_match = False, **kwargs):
        for root, dirs, files in os.walk(start_path, followlinks = True):
            dirs.sort()
    
            # how deep we are into the tree (root = 0)
            level = 0
            
            if root != start_path:
                level = root.replace(start_path, '').count(os.sep) + 1
    
            # stopping point - no more columns to check against
            if level >= len(columns):
                # yield turns this function into a generator instead of manually constructing
                # and returning a list then looping through it later
                # the code will resume from here when the next entry is required
                yield root.replace(start_path, ''), dirs
    
                # stops the os.walk from travelling any deeper
                dirs.clear()
    
            # match directory names against given filters to stop os.walk from finding unwanted datasets
            else:
                # if key wasn't provided, nothing will be filtered out
                match_values(dirs, columns[level], kwargs, exact_match, in_place = True)
                
    all_data = dataset_info_collection()

    for format_dirs in format_dirs_list:

        # find root of string by looking for first variable
        if "{" in format_dirs:
            first_arg_pos = format_dirs.find("{")
    
            # cut off by folder in case the variable wasn't immediately after /
            slash_pos = format_dirs[first_arg_pos::-1].find(os.sep)
            if slash_pos == -1:
                prev_separator_pos = -1
            else:
                prev_separator_pos = first_arg_pos - slash_pos
    
            start_path = format_dirs[:prev_separator_pos + 1]
            columns = format_dirs[prev_separator_pos + 1:].split(os.sep)
    
            # remove empty final column which shows up from the split if path string ended in /
            if not columns[-1]:
                columns.pop()
    
        else:
            start_path = format_dirs
            columns = []
        
            
        for root, dirs in filter_walk(start_path, columns, exact_match, **kwargs):
            info = extract_from_format(os.sep.join(columns), root)
            # if not info:
            #     info = {"path": format_dirs}

            for format_file in format_files_list:
                # print(start_path, root)
                # print(format_dirs.format(**info))
                # raise Exception("e")
                # dataset = dataset_info(info, format_dirs.format(**info), format_file)
                dataset = dataset_info(info, os.path.join(start_path, root) + os.sep, format_file)
                
                try:
                    dataset.get_info()
                except Exception as e:
                    print(e, info, root, columns)
                    # raise e
                    continue

                if dataset.any_files():
                    for key in kwargs:
                        if key not in dataset.data and key in dataset.info:
                            dataset = dataset.select(**{key: kwargs[key]}, exact_match = exact_match)

                    # check there are still files after selection
                    if dataset.any_files(): 
                        for item in all_data.items:
                            if item.attempt_merge(dataset):
                                break
                        else:
                            all_data.add(dataset)
                    
                    break

    if unique:
        for key, options in unique.items():
            all_data.prioritise(key, **options)

    # all_data = all_data.select(exact_match = exact_match, **kwargs)

    return all_data

    
def merge_values(values):
    """
    Merge a list of strings into a single nice string.
    This is mostly for the purpose of cleaning up a big list of years for readability.
    If not all inputs can be converted into integers, it will simply return a comma separated list.
    Otherwise, it will attempt to group integers (in the format "start to end") if they are consecutive. 
    If there are multiple non-continuous groups of integers, they will be comma separated.
    Example: If values = ["1960", "1961", "1962", "1963", "1964", "1970", "1971", "1972"],
    then the output will be "1960 to 1964, 1970 to 1972".
    Inputs:
    - values: the list of strings to merge
    Output:
    A single string joining the values as specified.
    """
    if all([item.isdigit() for item in values]):
        numbers = sorted([int(value) for value in values])
        start_number = numbers.pop(0)
        out_str = f"{start_number}"
        end_number = start_number
        previous_number = start_number
        while numbers:
            next_number = numbers.pop(0)
            if next_number == previous_number + 1:
                end_number = next_number
            else:
                if start_number != end_number:
                    out_str = out_str + f" to {end_number}"
                start_number = next_number
                end_number = start_number
                out_str = out_str + f", {start_number}"

            previous_number = next_number
                
        if start_number != end_number:
            out_str = out_str + f" to {end_number}"

        return out_str
        
    return ", ".join(values)


def year_range(start = None, end = None, step: int = 1, inclusive: bool = True):
    """
    Generates a list of year strings from between "start" and "end" for the purposes of string matching.
    In general, matches the output of range(start, end, step), but will behave slightly differently
    if only one input is given (it will be treated as the start instead of the end.
    Also, defaults to including the end point which range() does not.
    Inputs:
    - start: int representing start year. If not specified or None, defaults to 1800. Will attempt to convert if not type int
    - end: int representing end year. If not specified or None, defaults to 2200. Will attempt to convert if not type int
    - step: int representing step count. Defaults to 1 (i.e. include every year)
    - inclusive: bool for whether or not to include the end point. Defaults to True for ease of use
    Outputs:
    A list of strings according to the given parameters.
    """

    start = int(start)
    end = int(end)
    
    # default values, though there's probably a better solution here (with slices or maybe regexps up the line)
    if start is None:
        start = 1800
    if end is None:
        end = 2200
        
    return [str(year) for year in range(start, end + 1 if inclusive else 0, step)]


def paths(key, yaml_path = "paths.yml"):
    """
    Use a yaml file to record paths in advance and load them by key.
    Return a function that calls filter_all with the appropriate paths.
    Inputs:
    - key: The name of the paths being referenced within the yaml file
    - yaml_path: The path of the yaml file (default "paths.yml" in working directory)
    Returns:
    A function calling filter_all with the file path arguments already assigned    
    """
    if yaml_path[0] != os.sep:
        yaml_path = os.path.join(os.path.dirname(__file__), yaml_path)
        
    with open(yaml_path, 'r') as fstream:
        keys = yaml.safe_load(fstream)

    format_dirs = keys[key]["format_dirs"]
    format_file = keys[key]["format_file"]
    if "unique" in keys[key]:
        unique = keys[key]["unique"]
    else:
        unique = None

    def use_paths(exact_match = False, **kwargs):
        return filter_all(format_dirs, format_file, unique, exact_match, **kwargs)

    return use_paths


def get_datasets(key, yaml_path = "paths.yml", exact_match = False, **kwargs):
    """
    Use a yaml file to get path formats, then immediately search and return dataset matches.
    Identical to "paths" above except removes an intermediate step. See filter_all for more.
    Inputs:
    - key: The name of the paths being referenced within the yaml file
    - yaml_path: The path of the yaml file (default "paths.yml" in working directory)
    - exact_match: Whether to match search terms exactly, default to False. Otherwise a substring 
    is considered a match
    - **kwargs: Keyword arguments mapping search terms to values for matching. Multiple values can
    be assigned to each search term - only one needs to match for it to be included.
    Output:
    A dataset_info_collection object containing a list of dataset_info objects corresponding to
    successful matches.
    """
    return paths(key, yaml_path)(exact_match, **kwargs)

