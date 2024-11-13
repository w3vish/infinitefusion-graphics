const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

// Initialize SQLite database
const db = new sqlite3.Database('./infinitefusion.db');

// Create or update the table to store file metadata and credits
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS images (
      sprite_id TEXT PRIMARY KEY,
      sprite_type TEXT,
      base_id TEXT,
      creation_date TEXT,
      last_update_date TEXT,
      artist TEXT,
      comments TEXT
    )
  `);
});

// Modified getBaseId function
function getBaseId(sprite_id) {
  const baseIdMatch = sprite_id.match(/^\d+(\.\d+)*([a-zA-Z]?)/);
  return baseIdMatch ? baseIdMatch[0].replace(/[a-zA-Z]$/, '') : sprite_id.split(' by ')[0];
}

// Function to get file metadata asynchronously
const getFileMetadata = async (filePath) => {
  try {
    const stats = await fs.promises.stat(filePath);
    const creationDate = stats.birthtime.toISOString(); // Creation date
    const lastUpdateDate = stats.mtime.toISOString(); // Last modification date

    return {
      filePath,
      creationDate,
      lastUpdateDate
    };
  } catch (error) {
    console.error(`Error processing file ${filePath}: ${error}`);
    return null;
  }
};

// Extract the sprite ID from the file path
const extractSpriteId = (filePath) => {
  const fileName = path.basename(filePath); // Get the file name
  const match = fileName.match(/^(.+)\.png$/); // Match the name before '.png'
  return match ? match[1] : null;
};

function getSpriteType(spriteId) {
    // Check if the ID has an alphabetic character at the end
    const hasAlphaAtEnd = /[a-zA-Z]$/.test(spriteId);
    
    // Get base sprite type (numbers only)
    const baseType = spriteId.replace(/[a-zA-Z]$/, '');
    
    // If it's a pure numeric ID (with dots) it's 'main', if it has a letter at the end it's 'alt'
    return hasAlphaAtEnd ? 'alt' : 'main';
  }

// Insert metadata and credits into the database in batches
// Modified insert metadata batch function
const insertMetadataBatch = async (metadataBatch) => {
    const MAX_VARIABLES = 999;
    const ROW_SIZE = 7;
    const MAX_ROWS_PER_BATCH = Math.floor(MAX_VARIABLES / ROW_SIZE);
  
    for (let i = 0; i < metadataBatch.length; i += MAX_ROWS_PER_BATCH) {
      const batchChunk = metadataBatch.slice(i, i + MAX_ROWS_PER_BATCH);
  
      const placeholders = batchChunk.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(',');
      const values = batchChunk.reduce((acc, metadata) => {
        // Always determine sprite type based on ID format
        const spriteType = getSpriteType(metadata.spriteId);
        
        acc.push(
          metadata.spriteId,         // sprite_id
          spriteType,                // sprite_type (now correctly set as 'main' or 'alt')
          getBaseId(metadata.spriteId), // base_id
          metadata.creationDate,     // creation_date
          metadata.lastUpdateDate,   // last_update_date
          metadata.artist,           // artist
          metadata.comments          // comments
        );
        return acc;
      }, []);
  
      const query = `
        INSERT OR REPLACE INTO images (
          sprite_id,
          sprite_type,
          base_id,
          creation_date,
          last_update_date,
          artist,
          comments
        ) VALUES ${placeholders}
      `;
  
      await new Promise((resolve, reject) => {
        db.run(query, values, (err) => {
          if (err) {
            console.error(`Error inserting metadata batch: ${err}`);
            reject(err);
          } else {
            resolve();
          }
        });
      });
    }
  };

// List all files in a directory, filter out non-files
const listFilesInDirectory = async (directory) => {
  try {
    const files = await fs.promises.readdir(directory);
    const filePaths = files.map((file) => path.join(directory, file));

    // Filter out directories and only return file paths
    const filteredFilePaths = await Promise.all(filePaths.map(async (file) => {
      const stat = await fs.promises.stat(file);
      return stat.isFile() ? file : null;
    }));

    return filteredFilePaths.filter(Boolean); // Remove any nulls (directories)
  } catch (err) {
    console.error(`Error reading directory ${directory}: ${err}`);
    return [];
  }
};

// Read the sprite credits CSV
const readCSV = async (filePath) => {
  try {
    const fileContent = await fs.promises.readFile(filePath, 'utf-8');
    const rows = fileContent.trim().split('\n');

    return rows.map((line) => {
      const [id, artist, sprite_type, comments] = line.split(',').map(item => item.trim());
      return { id, artist, sprite_type, comments };
    });
  } catch (error) {
    console.error(`Error reading CSV file ${filePath}: ${error}`);
    return [];
  }
};

// Directories and credits CSV file
const directories = ['CustomBattlers', 'other/BaseSprites', 'other/Triples'];
const creditsFile = 'Sprite Credits.csv';

let fileBatch = [];
const BATCH_SIZE = 10000;
let totalProcessedFiles = 0;

// Main function to process files and combine with credits
const processFilesInChunks = async () => {
  // Read the credits CSV into memory
  const spriteCredits = await readCSV(creditsFile);
  const creditsMap = new Map(spriteCredits.map((credit) => [credit.id, credit]));

  // Iterate through each directory
  for (const directory of directories) {
    console.log(`Processing files in directory: ${directory}`);

    const files = await listFilesInDirectory(directory); // List files in the directory

    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const metadata = await getFileMetadata(file);

      if (metadata) {
        const spriteId = extractSpriteId(file);
        if (spriteId) {
          // Fetch credits for this sprite ID if available
          const credits = creditsMap.get(spriteId) || {};

          fileBatch.push({
            spriteId,
            sprite_type: credits.sprite_type || null,
            creationDate: metadata.creationDate,
            lastUpdateDate: metadata.lastUpdateDate,
            artist: credits.artist || null,
            comments: credits.comments || null
          });

          totalProcessedFiles++;
        }
      }

      // If batch size reaches limit, process and clear batch
      if (fileBatch.length >= BATCH_SIZE) {
        console.log(`Processing batch of ${BATCH_SIZE} files...`);
        await insertMetadataBatch(fileBatch);
        fileBatch = []; // Clear the batch
      }
    }
  }

  // Process any remaining files
  if (fileBatch.length > 0) {
    console.log(`Processing remaining ${fileBatch.length} files...`);
    await insertMetadataBatch(fileBatch);
  }

  // Output total number of processed files
  console.log(`Total processed files: ${totalProcessedFiles}`);

  // Close the database connection
  db.close(() => {
    console.log('Database connection closed.');
  });
};

// Start processing
processFilesInChunks().catch((err) => {
  console.error(`Error in processing: ${err}`);
  db.close(() => {
    console.log('Database connection closed due to error.');
  });
});