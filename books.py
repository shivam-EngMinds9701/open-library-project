import requests
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import os
import dotenv
import numpy as np

dotenv.load_dotenv()


class BookFetcher:
    def __init__(
        self,
        subject="fiction",
        fields="title,author_name,first_publish_year,ratings_sortable",
        sort_by="rating",
        limit=100,
    ):
        """
        Initializes a BookFetcher object.

        Args:
            subject (str, optional): The subject of the books to fetch. Defaults to "fiction".
            fields (str, optional): The fields to include in the fetched books. Defaults to "title,author_name,first_publish_year,ratings_sortable".
            sort_by (str, optional): The field to sort the fetched books by. Defaults to "rating".
            limit (int, optional): The maximum number of books to fetch. Defaults to 100.

        Returns:
            None
        """
        self.base_url = "https://openlibrary.org"
        self.query = f"/search.json?subject={subject}&fields={fields}&sort={sort_by}&limit={limit}"

    def fetch_books(self):
        """
        Fetches books from the Open Library API based on the specified query parameters.

        Returns:
            list: A list of dictionaries representing the fetched books.

        Raises:
            requests.exceptions.HTTPError: If the request to the API fails.

        Prints:
            Total number of books: <int>: The total number of books found.
            Limiting to top 100 books:
        """

        response = requests.get(self.base_url + self.query)
        if response.status_code == 200:
            data = response.json()
            print("Total number of books: ", data["numFound"])
            print("Limiting to top 100 books", end="\n\n")
            return data["docs"]
        else:
            response.raise_for_status()


class BookCleaner:
    def __init__(self, raw_books):
        """
        Initializes a new instance of the class.

        Parameters:
            raw_books (list): A list of raw books data.

        Returns:
            None

        This function initializes the `raw_books` attribute with the provided `raw_books` parameter. It also sets the `cleaned_data` attribute to `None`.
        """
        self.raw_books = raw_books
        self.cleaned_data = None

    def process_data(self):
        """
        Processes the raw book data and performs the following operations:

        1. Iterates over each book in the `raw_books` list.
        2. Extracts the title, author, first publish year, and rating from each book.
        3. Filters out books with missing data (title, author, or first publish year).
        4. Appends the cleaned book data to the `books` list.
        5. Converts the `books` list into a pandas DataFrame and assigns it to the `cleaned_data` attribute.
        6. Writes the `cleaned_data` DataFrame to a CSV file named "books.csv" without an index.
        7. Writes the `cleaned_data` DataFrame to a JSON file named "books.json" in "records" format.

        This function does not take any parameters and does not return anything.
        """
        books = []
        for book in self.raw_books:
            title = book.get("title")
            author = ", ".join(book.get("author_name", []))
            first_publish_year = book.get("first_publish_year")
            rating = book.get("ratings_sortable", "")

            # Filter out books with missing data
            if title and author and first_publish_year and rating:
                books.append(
                    {
                        "title": title,
                        "author": author,
                        "first_publish_year": first_publish_year,
                        "rating": rating,
                    }
                )

            self.cleaned_data = pd.DataFrame(books)

            # Write to CSV & JSON
            self.cleaned_data.to_csv("books.csv", index=False)
            self.cleaned_data.to_json("books.json", orient="records")

    def clean_data(self):
        """
        Cleans the data by removing missing values and duplicates.

        Raises:
            ValueError: If the data has not been processed yet. Call process_data() before cleaning data.

        Returns:
            pandas.DataFrame: The cleaned data.

        Prints:
            - Missing values in each column.
            - Number of duplicate rows.
            - Fixed <duplicates> duplicates and dropped <missing_values.sum()> missing values.
            - A preview of the cleaned data.
        """
        if self.cleaned_data is None:
            raise ValueError(
                "Data has not been processed yet. Call process_data() before cleaning data."
            )

        # Checking for missing values
        missing_values = self.cleaned_data.isnull().sum()
        print("Missing values in each column:\n", missing_values, end="\n\n")

        # Check for duplicates
        duplicates = self.cleaned_data.duplicated().sum()
        print("Number of duplicate rows: ", duplicates, end="\n\n")

        # Drop rows with any missing values
        self.cleaned_data.dropna(inplace=True)

        # Drop duplicate rows
        self.cleaned_data.drop_duplicates(inplace=True)

        # Loading cleaned data
        print(
            f"Fixed {duplicates} duplicates and dropped {missing_values.sum()} missing values.",
            end="\n\n",
        )

        print("Here's a preview of the cleaned data:\n")
        print(self.cleaned_data.head(), end="\n\n")

        return self.cleaned_data


class BookDatabase:
    def __init__(self, cleaned_data):
        """
        Initializes a new instance of the class.

        Parameters:
            cleaned_data (pandas.DataFrame): The cleaned data to be stored in the instance.

        Returns:
            None

        Initializes the instance with the provided cleaned data and creates a connection to the database using the environment variable DB_CONN_STRING.
        """
        self.cleaned_data = cleaned_data
        self.conn_string = os.getenv("DB_CONN_STRING")
        self.engine = create_engine(self.conn_string)

    def save_data(self):
        """
        Save the cleaned data to Azure SQL DB.

        This method saves the cleaned data to the "books" table in the Azure SQL DB using the `to_sql` method of the `pandas.DataFrame` class. The data is saved with the "replace" option, which means that if the table already exists, it will be replaced with the new data. The index is set to `False` to exclude the index column from the saved data.

        Returns:
            None

        Raises:
            Exception: If there is an error saving the data.

        Prints:
            - "Data saved successfully! to Azure SQL DB." if the data is saved successfully.
            - "Error saving data: <error_message>" if there is an error saving the data.
        """
        print("Saving data...", end="\n\n")
        try:
            self.cleaned_data.to_sql(
                "books", self.engine, if_exists="replace", index=False
            )
            print("Data saved successfully! to Azure SQL DB.\n\n")
        except Exception as e:
            print(f"Error saving data: {e}")

    def fetch_data(self):
        """
        Fetches data from Azure SQL DB.

        This function executes a SQL query to fetch all records from the 'books' table in the Azure SQL DB.
        It uses the `pd.read_sql()` function from the pandas library to execute the query and retrieve the data.

        Returns:
            pandas.DataFrame: The fetched data as a pandas DataFrame.

        Raises:
            Exception: If there is an error fetching the data.

        Prints:
            - "Data fetched successfully! from Azure SQL DB." if the data is fetched successfully.
            - "Error fetching data: <error_message>" if there is an error fetching the data.
            - A preview of the fetched data.
        """
        try:
            query = "SELECT * FROM books"
            df = pd.read_sql(query, self.engine)
            print("Data fetched successfully! from Azure SQL DB.\n\n")
            print("Here's a preview of the fetched data:\n")
            print(df.head(), end="\n\n")
            return df
        except Exception as e:
            print(f"Error fetching data: {e}")


class BookVisualizer:
    def __init__(self, df):
        """
        Initializes an instance of the class with a DataFrame and sets the default figure size.

        Parameters:
            df (pandas.DataFrame): The DataFrame to be used for analysis.

        Returns:
            None
        """
        self.df = df
        self.figsize = (15, 10)

    def visualize_data(self):
        """
        Visualizes the data by creating a countplot of the number of books published by year.

        This function sets the theme, figure size, font scale, color palette, and creates a countplot of the number of books published by year. It then sets the labels, rotates the x-axis labels, saves the figure as a PNG file, and prints a message indicating the location of the saved file.

        Parameters:
            self (BookVisualizer): The current instance of the BookVisualizer class.

        Returns:
            None
        """
        print("Visualizing data...", end="\n\n")

        # Set theme
        plt.style.use("fivethirtyeight")

        # Set figure size
        plt.figure(figsize=self.figsize)

        # Set font scale
        sns.set_context(rc={"lines.linewidth": 1.5})

        # Set color palette
        sns.set_palette("viridis")

        # Plot countplot
        sns.countplot(data=self.df, x="first_publish_year")

        # Set labels
        plt.title("Number of Books Published by Year")
        plt.xlabel("Year")
        plt.ylabel("Number of Books")

        # Rotate x-axis labels
        plt.xticks(rotation=45)

        # Save figure
        plt.savefig("./visuals/countplot.png")
        print("Countplot saved to ./visuals/countplot.png")


def main():
    """
    The main function that fetches data, processes it, cleans it, saves it to Azure SQL DB, fetches it from Azure SQL DB, and visualizes it.

    This function fetches the raw book data using the `fetch_books` method of the `BookFetcher` class. It then passes the raw books to the `BookCleaner` class to process the data. The processed data is cleaned by removing missing values and duplicates using the `clean_data` method of the `BookCleaner` class. The cleaned data is then saved to Azure SQL DB using the `save_data` method of the `BookDatabase` class. The data is fetched from Azure SQL DB using the `fetch_data` method of the `BookDatabase` class. Finally, the fetched data is visualized using the `visualize_data` method of the `BookVisualizer` class.

    This function does not take any parameters and does not return anything.
    """

    # Fetch data
    fetcher = BookFetcher()
    raw_books = fetcher.fetch_books()

    # Process data
    cleaner = BookCleaner(raw_books)
    cleaner.process_data()

    # Clean data
    cleaned_data = cleaner.clean_data()

    # Save data to Azure SQL DB
    db = BookDatabase(cleaned_data)
    # db.save_data()

    # Fetch data from Azure SQL DB
    df = db.fetch_data()

    # Visualize data
    visualizer = BookVisualizer(df)
    visualizer.visualize_data()


if __name__ == "__main__":
    main()
