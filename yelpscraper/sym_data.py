import pandas
class get_codes():
    def __init__(self, path, code):
        self.code = code
        self.symbols = pandas.read_csv(path, delimiter='\t')
        self.symbols = self.symbols.set_index('symbol')
        self.symbols = self.symbols.T.to_dict()

    def replace_sym(self, word):
        word = list(word)
        syms = self.symbols.keys()
        new_word = ''.join(map(lambda alp: alp if alp not in syms else self.symbols[alp][self.code], word))
        return(new_word)

if __name__ == "__main__":
    path = 'symbols.tsv'
    encoder = get_codes(path, 'hex')
    word='hgij5 jjf'
    print(encoder.replace_sym(word))