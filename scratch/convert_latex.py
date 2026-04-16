import re

def convert(md_text):
    latex_header = r"""\documentclass[a4paper,12pt]{report}
\usepackage[utf8]{inputenc}
\usepackage[T2A]{fontenc}
\usepackage[russian]{babel}
\usepackage{amsmath}
\usepackage{listings}
\usepackage{xcolor}
\usepackage{tabularx}
\usepackage{longtable}
\usepackage{hyperref}

\begin{document}

"""

    latex_footer = r"""
\end{document}
"""

    lines = md_text.split('\n')
    out_lines = []
    
    in_code_block = False
    in_table = False
    
    for line in lines:
        # Check for code blocks
        if line.startswith('```'):
            if in_code_block:
                out_lines.append(r'\end{verbatim}')
                in_code_block = False
            else:
                out_lines.append(r'\begin{verbatim}')
                in_code_block = True
            continue
            
        if in_code_block:
            out_lines.append(line)
            continue
            
        # At this point we are outside a code block. We can apply inline replacements.
        
        # Bold
        line = re.sub(r'\*\*(.*?)\*\*', r'\\textbf{\1}', line)
        # Italic (only matching _..._ or *...* that are words)
        line = re.sub(r'\b_(.*?)_\b', r'\\textit{\1}', line)
        
        # Inline code
        line = re.sub(r'`(.*?)`', lambda m: r'\texttt{' + m.group(1).replace('_', r'\_') + r'}', line)
            
        if line.strip().startswith('|'):
            if not in_table:
                # Count columns
                cols = line.count('|') - 1
                out_lines.append(r'\begin{longtable}{|' + 'l|'*cols + '}')
                out_lines.append(r'\hline')
                in_table = True
            
            # If it's the separator line |:---|:---|
            if '---' in line:
                continue
                
            cells = [c.strip() for c in line.split('|')[1:-1]]
            
            # Escape underscore and ampersands in table cells if they are not in texttt
            escaped_cells = []
            for c in cells:
                if '\\texttt{' not in c and '$' not in c:
                    c = re.sub(r'(?<!\\)_', r'\_', c)
                    c = re.sub(r'(?<!\\)&', r'\&', c)
                escaped_cells.append(c)
                
            out_lines.append(' & '.join(escaped_cells) + r' \\ \hline')
            continue
        elif in_table:
            out_lines.append(r'\end{longtable}')
            in_table = False
            
        # Headers
        if line.startswith('# ГЛАВА'):
            header_text = line.replace('# ГЛАВА 1. ', '').strip()
            out_lines.append(r'\chapter{' + header_text + '}')
            continue
            
        if line.startswith('## '):
            header_text = line[3:].strip()
            # remove 1.x. numbering if present
            header_text = re.sub(r'^1\.\d+\.\s*', '', header_text)
            out_lines.append(r'\section{' + header_text + '}')
            continue
            
        if line.startswith('### '):
            header_text = line[4:].strip()
            out_lines.append(r'\subsection{' + header_text + '}')
            continue

        if line == '---':
            out_lines.append(r'\vspace{0.5cm}\hrule\vspace{0.5cm}')
            continue
            
        # Math blocks
        if line.startswith('$$') and line.endswith('$$'):
            out_lines.append(r'\begin{equation*}')
            out_lines.append(line[2:-2])
            out_lines.append(r'\end{equation*}')
            continue
            
        # Escape characters outside code blocks and outside math
        if '$' not in line and '\\texttt' not in line:
            line = re.sub(r'(?<!\\)%(?!\S)', r'\%', line) # escape % if not preceded by \
            line = re.sub(r'(?<!\\)_', r'\_', line)
            
        out_lines.append(line)

    if in_table:
        out_lines.append(r'\end{longtable}')

    return latex_header + '\n'.join(out_lines) + latex_footer


if __name__ == '__main__':
    md_path = '/thesis/chapter1/chapter1_full.md'
    tex_path = 'c:/Users/Lenovo/PycharmProjects/SQL_engine/thesis/chapter1_full.tex'
    with open(md_path, 'r', encoding='utf-8') as f:
        md = f.read()
    
    latex = convert(md)
    
    with open(tex_path, 'w', encoding='utf-8') as f:
        f.write(latex)
