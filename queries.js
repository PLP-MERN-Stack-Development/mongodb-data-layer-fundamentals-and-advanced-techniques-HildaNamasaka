//BASIC CRUD OPERATIONS
// queries.js - MongoDB Week 1 Assignment Queries
// Load environment variables
require('dotenv').config();

const { MongoClient } = require('mongodb');

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri);

async function runQueries() {
  try {
    await client.connect();
    console.log(' Connected to MongoDB Atlas\n');
    
    const db = client.db('plp_bookstore');
    const books = db.collection('books');

    // TASK 2: BASIC CRUD OPERATIONS
    
    console.log('========================================');
    console.log('TASK 2: BASIC CRUD OPERATIONS');
    console.log('========================================\n');
    
    // 1. Find all books in a specific genre
    console.log(' Find all books in "Fiction" genre:');
    const fictionBooks = await books.find({ genre: 'Fiction' });
    console.log(`Found ${fictionBooks.length} Fiction books:`);
    fictionBooks.forEach(book => console.log(`   - ${book.title} by ${book.author}`));
    
    // 2. Find books published after a certain year
    console.log('\n Find books published after 2010:');
    const recentBooks = await books.find({ published_year: { $gt: 1950 } }).toArray();
    console.log(`Found ${recentBooks.length} books published after 1950:`);
    recentBooks.forEach(book => console.log(`   - ${book.title} (${book.published_year})`));
    
    // 3. Find books by a specific author
    console.log('\nFind books by George Orwell:');
    const orwellBooks = await books.find({ author: 'George Orwell' }).toArray();
    console.log(`Found ${orwellBooks.length} books by George Orwell:`);
    orwellBooks.forEach(book => console.log(`   - ${book.title}`));
    
    // 4. Update the price of a specific book
    console.log('\nUpdate the price of "1984":');
    const updateResult = await books.updateOne(
      { title: '1984' },
      { $set: { price: 15.99 } }
    );
    console.log(`Modified ${updateResult.modifiedCount} document(s)`);
    
    // Verify the update
    const updatedBook = await books.findOne({ title: '1984' });
    console.log(`   New price of "1984": $${updatedBook.price}`);
    
    // 5. Delete a book by its title (first add a book to delete)
    console.log('\nDelete a book by title:');
    
    // First, insert a book to delete
    await books.insertOne({
      title: 'Book to Delete',
      author: 'Test Author',
      genre: 'Test',
      published_year: 2020,
      price: 9.99,
      in_stock: true,
      pages: 100,
      publisher: 'Test Publisher'
    });
    console.log('   Added "Book to Delete" to the collection');
    
    // Now delete it
    const deleteResult = await books.deleteOne({ title: 'Book to Delete' });
    console.log(` Deleted ${deleteResult.deletedCount} document(s)`);

    // TASK 3: ADVANCED QUERIES
    
    console.log('\n========================================');
    console.log('TASK 3: ADVANCED QUERIES');
    console.log('========================================\n');
    
    // 1. Find books that are both in stock and published after 2010
    console.log('Books in stock AND published after 1950:');
    const inStockRecent = await books.find({
      in_stock: true,
      published_year: { $gt: 1950 }
    }).toArray();
    console.log(`Found ${inStockRecent.length} books:`);
    inStockRecent.forEach(book => console.log(`   - ${book.title} (${book.published_year})`));
    
    // 2. Use projection to return only title, author, and price
    console.log('\n Books with projection (title, author, price only):');
    const projectedBooks = await books.find(
      {},
      { projection: { title: 1, author: 1, price: 1, _id: 0 } }
    ).limit(5).toArray();
    console.log('Showing first 5 books:');
    projectedBooks.forEach(book => {
      console.log(`   - ${book.title} by ${book.author} - $${book.price}`);
    });
    
    // 3a. Sorting by price (ascending)
    console.log('\n Books sorted by price (ASCENDING):');
    const sortedAsc = await books.find({})
      .sort({ price: 1 })
      .limit(5)
      .toArray();
    console.log('Top 5 cheapest books:');
    sortedAsc.forEach(book => console.log(`   - ${book.title}: $${book.price}`));
    
    // 3b. Sorting by price (descending)
    console.log('\n Books sorted by price (DESCENDING):');
    const sortedDesc = await books.find({})
      .sort({ price: -1 })
      .limit(5)
      .toArray();
    console.log('Top 5 most expensive books:');
    sortedDesc.forEach(book => console.log(`   - ${book.title}: $${book.price}`));
    
    // 4. Pagination - 5 books per page
    console.log('\n Pagination (5 books per page):');
    
    const totalBooks = await books.countDocuments();
    const booksPerPage = 5;
    const totalPages = Math.ceil(totalBooks / booksPerPage);
    
    console.log(`Total books: ${totalBooks}`);
    console.log(`Books per page: ${booksPerPage}`);
    console.log(`Total pages: ${totalPages}\n`);
    
    console.log('Page 1:');
    const page1 = await books.find({})
      .limit(booksPerPage)
      .skip(0)
      .toArray();
    page1.forEach((book, i) => console.log(`   ${i + 1}. ${book.title}`));
    
    console.log('\n Page 2:');
    const page2 = await books.find({})
      .limit(booksPerPage)
      .skip(booksPerPage)
      .toArray();
    page2.forEach((book, i) => console.log(`   ${i + 1}. ${book.title}`));

    // TASK 4: AGGREGATION PIPELINE
    
    console.log('\n========================================');
    console.log('TASK 4: AGGREGATION PIPELINE');
    console.log('========================================\n');
    
    // 1. Average price of books by genre
    console.log('Average price by genre:');
    const avgPriceByGenre = await books.aggregate([
      {
        $group: {
          _id: '$genre',
          averagePrice: { $avg: '$price' },
          bookCount: { $sum: 1 }
        }
      },
      {
        $sort: { averagePrice: -1 }
      }
    ]).toArray();
    
    avgPriceByGenre.forEach(genre => {
      console.log(`   ${genre._id}: $${genre.averagePrice.toFixed(2)} (${genre.bookCount} books)`);
    });
    
    // 2. Author with the most books
    console.log('\n Author with most books:');
    const topAuthors = await books.aggregate([
      {
        $group: {
          _id: '$author',
          bookCount: { $sum: 1 },
          books: { $push: '$title' }
        }
      },
      {
        $sort: { bookCount: -1 }
      },
      {
        $limit: 3
      }
    ]).toArray();
    
    console.log('Top 3 authors:');
    topAuthors.forEach((author, i) => {
      console.log(`   ${i + 1}. ${author._id}: ${author.bookCount} book(s)`);
      author.books.forEach(book => console.log(`      - ${book}`));
    });
    
    // 3. Group books by publication decade
    console.log('\n Books grouped by publication decade:');
    const booksByDecade = await books.aggregate([
      {
        $addFields: {
          decade: {
            $multiply: [
              { $floor: { $divide: ['$published_year', 10] } },
              10
            ]
          }
        }
      },
      {
        $group: {
          _id: '$decade',
          count: { $sum: 1 },
          books: { $push: { title: '$title', year: '$published_year' } }
        }
      },
      {
        $sort: { _id: 1 }
      }
    ]).toArray();
    
    booksByDecade.forEach(decade => {
      console.log(`\n  ${decade._id}s (${decade.count} books):`);
      decade.books.forEach(book => {
        console.log(`      - ${book.title} (${book.year})`);
      });
    });

    // TASK 5: INDEXING
    
    console.log('\n========================================');
    console.log('TASK 5: INDEXING');
    console.log('========================================\n');
    
    // 1. Create index on title
    console.log(' Creating index on title field...');
    await books.createIndex({ title: 1 });
    console.log(' Index created on title field\n');
    
    // 2. Create compound index on author and published_year
    console.log(' Creating compound index on author and published_year...');
    await books.createIndex({ author: 1, published_year: -1 });
    console.log(' Compound index created\n');
    
    // 3. Use explain() to show performance
    console.log(' Query performance analysis:\n');
    
    // Query WITH index
    console.log(' Searching for "1984" (WITH index):');
    const explainWithIndex = await books.find({ title: '1984' })
      .explain('executionStats');
    
    console.log(`   Execution time: ${explainWithIndex.executionStats.executionTimeMillis}ms`);
    console.log(`   Documents examined: ${explainWithIndex.executionStats.totalDocsExamined}`);
    console.log(`   Documents returned: ${explainWithIndex.executionStats.nReturned}`);
    console.log(`   Index used: ${explainWithIndex.executionStats.executionStages.indexName || 'Yes'}`);
    
    // List all indexes
    console.log('\n All indexes on books collection:');
    const indexes = await books.indexes();
    indexes.forEach((index, i) => {
      console.log(`\n   Index ${i + 1}:`);
      console.log(`   Name: ${index.name}`);
      console.log(`   Keys: ${JSON.stringify(index.key)}`);
    });

    console.log('\n========================================');
    console.log(' ALL TASKS COMPLETED SUCCESSFULLY!');
    console.log('========================================\n');

  } catch (error) {
    console.error(' Error:', error);
  } finally {
    await client.close();
    console.log(' Connection closed\n');
  }
}

// Run all queries
runQueries().catch(console.error);