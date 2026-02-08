-- Migration: Change Author primary key from auto-increment to shamela_author_id
-- This makes Author consistent with Book (both use Shamela IDs as primary keys)

-- Step 1: Ensure all authors have a shamela_author_id (required for new PK)
-- If any don't, generate one based on their current id
UPDATE authors
SET shamela_author_id = CAST(id AS TEXT)
WHERE shamela_author_id IS NULL;

-- Step 2: Add new string column for author reference in books
ALTER TABLE books ADD COLUMN author_id_new TEXT;

-- Step 3: Populate the new column with shamela_author_id from authors
UPDATE books
SET author_id_new = authors.shamela_author_id
FROM authors
WHERE books.author_id = authors.id;

-- Step 4: Add new string column for author reference in author_works
ALTER TABLE author_works ADD COLUMN author_id_new TEXT;

-- Step 5: Populate author_works new column
UPDATE author_works
SET author_id_new = authors.shamela_author_id
FROM authors
WHERE author_works.author_id = authors.id;

-- Step 6: Drop old foreign key constraints
ALTER TABLE books DROP CONSTRAINT IF EXISTS books_author_id_fkey;
ALTER TABLE author_works DROP CONSTRAINT IF EXISTS author_works_author_id_fkey;

-- Step 7: Drop old author_id columns
ALTER TABLE books DROP COLUMN author_id;
ALTER TABLE author_works DROP COLUMN author_id;

-- Step 8: Rename new columns to author_id
ALTER TABLE books RENAME COLUMN author_id_new TO author_id;
ALTER TABLE author_works RENAME COLUMN author_id_new TO author_id;

-- Step 9: Drop the old authors primary key and create new one
-- First, drop the old primary key constraint
ALTER TABLE authors DROP CONSTRAINT authors_pkey;

-- Step 10: Drop the old id column and rename shamela_author_id to id
ALTER TABLE authors DROP COLUMN id;
ALTER TABLE authors RENAME COLUMN shamela_author_id TO id;

-- Step 11: Add the new primary key
ALTER TABLE authors ADD PRIMARY KEY (id);

-- Step 12: Make author_id NOT NULL in books (it was required before)
ALTER TABLE books ALTER COLUMN author_id SET NOT NULL;

-- Step 13: Add foreign key constraints back
ALTER TABLE books
ADD CONSTRAINT books_author_id_fkey
FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE author_works
ADD CONSTRAINT author_works_author_id_fkey
FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE ON UPDATE CASCADE;

-- Step 14: Recreate indexes
DROP INDEX IF EXISTS authors_shamela_author_id_idx;
CREATE INDEX books_author_id_idx ON books(author_id);
CREATE INDEX author_works_author_id_idx ON author_works(author_id);
