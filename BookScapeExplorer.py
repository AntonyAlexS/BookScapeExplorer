import streamlit as st
import requests
import pymysql
import pandas as pd

# Establish a connection to the MySQL database
def get_db_connection():
    return pymysql.connect(
        host="HostName",
        port=4000,
        user="UserName",
        password="Password",
        database="book",
        ssl_verify_cert=True,
        ssl_verify_identity=True,
        ssl_ca=r"Path/isrgrootx1.pem"
    )

# Function to run SQL query
def run_query(query):
    conn = get_db_connection()
    mycursor = conn.cursor()
    try:
        mycursor.execute(query)
        results = mycursor.fetchall()
        columns = [i[0] for i in mycursor.description]
        df = pd.DataFrame(results, columns=columns)
        conn.close()
        return df
    except pymysql.Error as err:
        st.error(f"Error executing query: {err}")
        conn.close()
        return None

# Streamlit App layout for collecting data from Google Books API and store it in the database   
def collect_and_store_books_data():
    apikey = 'API_Key'
    url = 'https://www.googleapis.com/books/v1/volumes'
    
    # Get user inputs
    search_key = st.text_input("Enter search key of the books:")
    total_books = st.number_input("Enter total number of books:", min_value=10, step=10)
    startIndex = 0
    maxResults = 10
    
    if st.button("Collect and Store Data"):
        books = []
        try:
            while len(books) < total_books:
                data = requests.get(url, params={"key": apikey, "q": search_key, "maxResults": maxResults, "startIndex": startIndex}).json()
                if 'items' not in data:
                    st.warning("No books found in response.")
                    break
                for item in data['items']:
                    book_details = {
                        'book_id': item.get('id', 'N/A'),
                        'search_key': search_key,
                        'book_title': item.get('volumeInfo', {}).get('title', 'N/A'),
                        'book_subtitle': item.get('volumeInfo', {}).get('subtitle', 'N/A'),
                        'book_authors': ", ".join(item.get('volumeInfo', {}).get('authors', [])) if isinstance(item.get('volumeInfo', {}).get('authors', []), list) else 'N/A',
                        'book_description': item.get('volumeInfo', {}).get('description', 'N/A'),
                        'industryIdentifiers': ", ".join([identifier['identifier'] for identifier in item.get('volumeInfo', {}).get('industryIdentifiers', [])]) if isinstance(item.get('volumeInfo', {}).get('industryIdentifiers', []), list) else 'N/A',
                        'text_readingModes': item.get('volumeInfo', {}).get('readingModes', {}).get('text', 'N/A'),
                        'image_readingModes': item.get('volumeInfo', {}).get('readingModes', {}).get('image', 'N/A'),
                        'pageCount': item.get('volumeInfo', {}).get('pageCount', 0),
                        'categories': ", ".join(item.get('volumeInfo', {}).get('categories', [])) if isinstance(item.get('volumeInfo', {}).get('categories', []), list) else 'N/A',
                        'language': item.get('volumeInfo', {}).get('language', 'N/A'),
                        'imageLinks': item.get('volumeInfo', {}).get('imageLinks', 'N/A'),
                        'ratingsCount': item.get('volumeInfo', {}).get('ratingsCount', 0),
                        'averageRating': item.get('volumeInfo', {}).get('averageRating', 0),
                        'country': item.get('saleInfo', {}).get('country', 'N/A'),
                        'saleability': item.get('saleInfo', {}).get('saleability', 'N/A'),
                        'isEbook': item.get('saleInfo', {}).get('isEbook', False),
                        'amount_listPrice': item.get('saleInfo', {}).get('listPrice', {}).get('amount', 0),
                        'currencyCode_listPrice': item.get('saleInfo', {}).get('listPrice', {}).get('currencyCode', 'N/A'),
                        'amount_retailPrice': item.get('saleInfo', {}).get('retailPrice', {}).get('amount', 0),
                        'currencyCode_retailPrice': item.get('saleInfo', {}).get('retailPrice', {}).get('currencyCode', 'N/A'),
                        'buyLink': item.get('saleInfo', {}).get('buyLink', 'N/A'),
                        'year': item.get('volumeInfo', {}).get('publishedDate', 'N/A').split('-')[0] if item.get('volumeInfo', {}).get('publishedDate', '') else 'N/A',
                        'publisher': item.get('volumeInfo', {}).get('publisher', '')
                    }
                    books.append(book_details)
                startIndex += maxResults
        except pymysql.Error as err:
            st.warning(f"KeyError encountered: {err}")
            
        # Create DataFrame and remove duplicates
        df = pd.DataFrame(books)
        df = df.drop_duplicates(subset='book_id', keep='first')

        # Insert into the database
        conn = get_db_connection()
        mycursor = conn.cursor()

        for index, row in df.iterrows():
            sql = """INSERT INTO books (book_id, search_key, book_title, book_subtitle, book_authors, book_description, industryIdentifiers, text_readingModes, image_readingModes, 
                    pageCount, categories, language, imageLinks, ratingsCount, averageRating, country, saleability, isEbook, amount_listPrice, currencyCode_listPrice, 
                    amount_retailPrice, currencyCode_retailPrice, buyLink, year, publisher) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            val = (row['book_id'], row['search_key'], row['book_title'], row['book_subtitle'], row['book_authors'], row['book_description'], str(row['industryIdentifiers']), 
                   bool(row['text_readingModes']), bool(row['image_readingModes']), int(row['pageCount']), str(row['categories']), row['language'], str(row['imageLinks']), 
                   int(row['ratingsCount']), float(row['averageRating']), row['country'], row['saleability'], bool(row['isEbook']), float(row['amount_listPrice']), 
                   row['currencyCode_listPrice'], float(row['amount_retailPrice']), row['currencyCode_retailPrice'], row['buyLink'], row['year'], row['publisher'])
            try:
                mycursor.execute(sql, val)
                conn.commit()
            except pymysql.Error as err:
                st.error(f"Error inserting row {index}: {err}")
                conn.rollback()

        conn.close()
        st.success("Data collected and stored successfully!")

# Streamlit App layout for running SQL queries
# Allows the user to select and run SQL queries on the stored book data.
def query_books_data():
    st.title("Run SQL Queries on Books Data")
    
    question = st.selectbox("Choose a query", [
        "1.Check Availability of eBooks vs Physical Books",
        "2.Find the Publisher with the Most Books Published",
        "3.Identify the Publisher with the Highest Average Rating",
        "4.Get the Top 5 Most Expensive Books by Retail Price",
        "5.Find Books Published After 2010 with at Least 500 Pages",
        "6.List Books with Discounts Greater than 20%",
        "7.Find the Average Page Count for eBooks vs Physical Books",
        "8.Find the Top 3 Authors with the Most Books",
        "9.List Publishers with More than 10 Books",
        "10.Find the Average Page Count for Each Category",
        "11.Retrieve Books with More than 3 Authors",
        "12.Books with Ratings Count Greater Than the Average",
        "13.Books with the Same Author Published in the Same Year",
        "14.Books with a Specific Keyword in the Title",
        "15.Year with the Highest Average Book Price",
        "16.Count Authors Who Published 3 Consecutive Years",
        "17.Authors with books published in same year, different publishers",
        "18.Average retail price of ebooks and physical books",
        "19.Identify Books that are outliers",
        "20.Publisher with the highest average rating (more than 10 books)"
    ])
# Depending on the question chosen, an appropriate SQL query is selected
    if question:
        if question == "1.Check Availability of eBooks vs Physical Books":
            query = "SELECT isEbook, COUNT(*) AS book_count FROM books GROUP BY isEbook;"
        elif question == "2.Find the Publisher with the Most Books Published":
            query = "SELECT publisher, COUNT(*) AS book_count FROM books WHERE publisher IS NOT NULL AND publisher != '' GROUP BY publisher ORDER BY book_count DESC LIMIT 1;"
        elif question == "3.Identify the Publisher with the Highest Average Rating":  # 3 Question
            query = "SELECT publisher FROM books WHERE publisher IS NOT NULL AND publisher != '' AND averageRating = (SELECT MAX(averageRating) FROM books);"
        elif question == "4.Get the Top 5 Most Expensive Books by Retail Price":   # 4 Question
            query = "SELECT book_title, amount_retailPrice FROM books ORDER BY amount_retailPrice DESC LIMIT 5;"
        elif question == "5.Find Books Published After 2010 with at Least 500 Pages":   #5 question
            query = "SELECT book_title, year, pageCount FROM books WHERE year > 2010 AND pageCount >= 500;"
        elif question == "6.List Books with Discounts Greater than 20%":   # 6 Question
            query = "SELECT book_title, (amount_listPrice - amount_retailPrice) / amount_listPrice AS discount_percentage FROM books WHERE (amount_listPrice - amount_retailPrice) / amount_listPrice > 0.02;"
        elif question == "7.Find the Average Page Count for eBooks vs Physical Books":  #7 Question
            query = "SELECT isEbook, AVG(pageCount) AS avg_page_count FROM books GROUP BY isEbook;"
        elif question == "8.Find the Top 3 Authors with the Most Books": #8 Question
            query = "SELECT book_authors, COUNT(*) AS book_count FROM books where book_authors IS NOT NULL AND book_authors !='' GROUP BY book_authors ORDER BY book_count DESC LIMIT 3;"
        elif question == "9.List Publishers with More than 10 Books": #9 Question
            query = "SELECT publisher, COUNT(*) AS book_count FROM books where publisher IS NOT NULL AND publisher != ''  GROUP BY publisher HAVING book_count > 10;"
        elif question == "10.Find the Average Page Count for Each Category":  #10 Question
            query = "SELECT categories, AVG(pageCount) AS avg_page_count FROM books WHERE categories IS NOT NULL AND categories!= '' GROUP BY categories HAVING AVG(pageCount)>0;"
        elif question == "11.Retrieve Books with More than 3 Authors":  #11 Question
            query = "SELECT book_title, book_authors FROM books WHERE LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) + 1 > 3;"
        elif question == "12.Books with Ratings Count Greater Than the Average":  #12 Question
            query = "SELECT book_title, ratingsCount FROM books WHERE ratingsCount > (SELECT AVG(ratingsCount) FROM books);"
        elif question == "13.Books with the Same Author Published in the Same Year": # 13 Question
            query = "SELECT book_authors, year, COUNT(*) AS book_count FROM books where book_authors IS NOT NULL AND book_authors !='' AND year!='N/A' GROUP BY book_authors, year HAVING book_count > 1;"
        elif question == "14.Books with a Specific Keyword in the Title": # 14 Question
            st.write("Please enter a keyword in the search box.")
            query = f"SELECT * FROM books WHERE LOWER(book_title) LIKE LOWER('%{st.text_input('Enter a search key')}%');"
        elif question == "15.Year with the Highest Average Book Price": # 15 Question
            query = "SELECT year, AVG(amount_retailPrice) AS avg_price FROM books GROUP BY year ORDER BY avg_price DESC LIMIT 1;"
        elif question == "16.Count Authors Who Published 3 Consecutive Years": #16 Question
            query = "SELECT book_authors FROM books GROUP BY book_authors HAVING COUNT(DISTINCT year) >= 3 AND MAX(year) - MIN(year) = COUNT(DISTINCT year) - 1;"
        elif question == "17.Authors with books published in same year, different publishers": #17 Question
            query = "SELECT book_authors, year, COUNT(*) AS book_count FROM books GROUP BY book_authors, year HAVING COUNT(DISTINCT publisher) > 1;"
        elif question == "18.Average retail price of ebooks and physical books":  # 18 Question
            query = "SELECT (SELECT AVG(amount_retailPrice) FROM books WHERE isEbook = True) AS avg_ebook_price,(SELECT AVG(amount_retailPrice) FROM books WHERE isEbook = False) AS avg_physical_price;"
        elif question == "19.Identify Books that are outliers":  # 19 Question
            query = "SELECT book_title, averageRating, ratingsCount FROM books WHERE averageRating > (SELECT AVG(averageRating) + 2 * STDDEV(averageRating) FROM books);"
        elif question == "20.Publisher with the highest average rating (more than 10 books)": # 20 Question
            query = "SELECT publisher, AVG(averageRating) AS averageRating, COUNT(*) AS book_count FROM books GROUP BY publisher HAVING COUNT(*) > 10 ORDER BY averageRating DESC LIMIT 1;"
        
        results_df = run_query(query)
        if results_df is not None:
            results_df.index = range(1, len(results_df) + 1)
            st.write(results_df.style.hide(axis="index"))

# Streamlit app layout where users can select options.
def app():
    st.sidebar.title("BookScape Explorer")
    app_mode = st.sidebar.radio("Choose a mode", ["Collect and Store Data", "Run SQL Queries"])

    if app_mode == "Collect and Store Data":
        collect_and_store_books_data()
    elif app_mode == "Run SQL Queries":
        query_books_data()

# Run the Streamlit app
if __name__ == "__main__":
    app()
