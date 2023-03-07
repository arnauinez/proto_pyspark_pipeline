# %%
import json
import os

with open('pipeline.json') as json_file:
    pipeline = json.load(json_file)
def main():
    for key, value in pipeline.items():
        x=os.system(f"python {value}")
        if x != 0:
            print(f"Error in k:{key} v:{value}")
            break
        print(f'Exit code: {x}')
    print("Done!")

if __name__ == "__main__":
    main()