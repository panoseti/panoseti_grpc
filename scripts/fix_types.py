import sys
import re
import os

def fix_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    new_lines = []
    i = 0
    needs_any = False
    has_any = 'Any' in "".join(lines)
    
    generics = ['list', 'dict', 'Callable', 'Future', 'Task', 'Queue', 'CompletedProcess', 'Awaitable']

    while i < len(lines):
        line = lines[i]
        
        # Match function definition (including indented ones)
        # Match start of def
        stripped = line.lstrip()
        if stripped.startswith('def '):
            indent_count = len(line) - len(stripped)
            indent = line[:indent_count]
            
            # Extract name
            name_match = re.match(r'def\s+(\w+)\s*\(', stripped)
            if name_match:
                name = name_match.group(1)
                
                # Find the end of the function signature
                sig_lines = []
                j = i
                params_str = ""
                found_end = False
                while j < len(lines):
                    sig_lines.append(lines[j])
                    combined = "".join(sig_lines).strip()
                    
                    # Search for the closing ): optionally with a return type
                    end_match = re.search(r'\)\s*(->\s*[^:]+)?\s*:\s*$', combined)
                    if end_match:
                        if combined.count('(') == combined.count(')'):
                            found_end = True
                            first_paren = combined.find('(')
                            last_paren = combined.rfind(')')
                            params_str = combined[first_paren+1:last_paren]
                            
                            return_type_part = end_match.group(1)
                            has_return_type = return_type_part is not None
                            
                            # Process params
                            param_list = []
                            bracket_level = 0
                            current_param = ""
                            for char in params_str:
                                if char in '([{': bracket_level += 1
                                elif char in ')]}': bracket_level -= 1
                                if char == ',' and bracket_level == 0:
                                    param_list.append(current_param)
                                    current_param = ""
                                else:
                                    current_param += char
                            if current_param.strip():
                                param_list.append(current_param)
                                
                            new_params = []
                            for p in param_list:
                                p_strip = p.strip()
                                if not p_strip:
                                    new_params.append(p)
                                    continue
                                if p_strip in ['self', 'cls'] or ':' in p_strip:
                                    # Still want to fix generics in existing annotations
                                    for g in generics:
                                        p = re.sub(r'([:\->,\[])\s*\b(' + g + r')\b(?!\s*\[)', r'\1 \2[Any]', p)
                                        if f"{g}[Any]" in p: needs_any = True
                                    new_params.append(p)
                                else:
                                    needs_any = True
                                    if '=' in p_strip:
                                        parts = p_strip.split('=', 1)
                                        p_name = parts[0].strip()
                                        p_default = parts[1].strip()
                                        # Fix generics in default values if any (unlikely but safe)
                                        new_params.append(f" {p_name}: Any = {p_default}")
                                    else:
                                        new_params.append(f" {p_strip}: Any")
                            
                            new_params_str = ",".join(new_params)
                            if not has_return_type:
                                new_sig = f"{indent}def {name}({new_params_str}) -> None:\n"
                            else:
                                # Fix generics in return type
                                for g in generics:
                                    return_type_part = re.sub(r'([:\->,\[])\s*\b(' + g + r')\b(?!\s*\[)', r'\1 \2[Any]', return_type_part)
                                    if f"{g}[Any]" in return_type_part: needs_any = True
                                new_sig = f"{indent}def {name}({new_params_str}){return_type_part}:\n"
                            
                            new_lines.append(new_sig)
                            i = j + 1
                            break
                    j += 1
                    if j >= len(lines): break
                
                if found_end:
                    continue

        # Handle generics in normal lines
        modified_line = line
        for g in generics:
            def generic_replacer(m):
                nonlocal needs_any
                needs_any = True
                return f"{m.group(1)}{m.group(2)}[Any]"
            modified_line = re.sub(r'([:\->,\[])\s*\b(' + g + r')\b(?!\s*\[)', generic_replacer, modified_line)
        new_lines.append(modified_line)
        i += 1

    final_content = "".join(new_lines)
    if needs_any and not has_any:
        if 'from typing import' in final_content:
            if 'Any' not in final_content:
                final_content = re.sub(r'from typing import (.*)', r'from typing import Any, \1', final_content)
        else:
            if 'import ' in final_content:
                final_content = re.sub(r'^(import |from )', r'from typing import Any\n\1', final_content, count=1, flags=re.MULTILINE)
            else:
                final_content = "from typing import Any\n" + final_content
                
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(final_content)

if __name__ == "__main__":
    for f in sys.argv[1:]:
        if os.path.isfile(f):
            fix_file(f)
