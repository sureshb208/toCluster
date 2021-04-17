if __name__=='__main__':
    import os
    import sys
    print("-----------------------------Start test code")
    print(os.listdir(sys.argv[1] if len(sys.argv) > 1 else None))
    print('-----------------------------------------END')
