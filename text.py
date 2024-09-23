import re

"""
This is the function to calculate the overlap rate in the sentences.
"""

# Function to read sentences from the file
def read_sentences_from_file(file_path, length):
    try:
        with open(file_path, 'r') as file:
            text = file.read()
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return []

    # Remove all non-alphabetic characters and convert to lowercase
    cleaned_text = re.sub(r'[^a-zA-Z\s]', '', text).lower()
    
    # Split text into words
    words = cleaned_text.split()
    print(f"Total words extracted: {len(words)}")

    # Group words into sentences of length words each
    sentences = [' '.join(words[i:i+length]) for i in range(0, len(words), length)]
    print(f"Total sentences created: {len(sentences)}")

    return sentences

# Function to calculate the average rate of common words between consecutive sentences
def calculate_average_common_rate(sentences):
    if len(sentences) < 2:
        return 0

    total_common_rate = 0
    total_overlap = 0
    for i in range(1, len(sentences)):
        words_set_current = set(sentences[i].split())
        words_set_previous = set(sentences[i-1].split())
        common_words = words_set_current.intersection(words_set_previous)
        # print("new sentence comparison:")
        # print(words_set_current)
        # print(words_set_previous)
        # print(common_words)
        # total_common_rate += len(common_words) / len(words_set_previous)
        total_common_rate += len(common_words)
        if len(common_words) > 0:
            total_overlap += 1

    average_common_rate = total_common_rate / (len(sentences) - 1)
    proportion_of_overlapping_sentences = total_overlap / (len(sentences) - 1)
    return average_common_rate, proportion_of_overlapping_sentences

def main():
    file_path = 'collection.txt'
    results = []

    for sentence_length in range(1, 51):
        print(f"\nAnalyzing sentence length: {sentence_length}")
        sentences = read_sentences_from_file(file_path, sentence_length)
        
        if len(sentences) == 0:
            print("No sentences containing only alphabetic characters were found.")
            continue

        average_common_rate, proportion_of_overlapping_sentences = calculate_average_common_rate(sentences)
        results.append((sentence_length, average_common_rate, proportion_of_overlapping_sentences))

    # Print the results
    for length, avg_rate, overlap in results:
        print(f"Sentence length: {length}, Average common rate: {avg_rate:.2f}, Proportion of overlapping sentences: {overlap:.2f}")


if __name__ == "__main__":
    main()
