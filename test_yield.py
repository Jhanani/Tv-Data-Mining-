

def generate_dict():
    for i in range(0,5):
        yield {i:i*i}

if __name__=='__main__':
    for m in generate_dict():
        print m.items()
