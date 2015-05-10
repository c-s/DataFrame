{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverlappingInstances #-}

module DataFrame (
 DataTable,
 createDataTableFromPair,
 createDataTable
 --(:=)

 ) where

import qualified Data.Vector as V
import qualified Data.Vector.Storable as SV
import Data.Dynamic
import Data.ByteString (ByteString)
-- import Data.String (IsString, fromString)

-- import Data.Typeable

--
--data VectorWithNA a where
--  VectorWithNA :: Double -> VectorWithNA Double = VectorWithNADouble (SV.Vector Double) deriving (Show, Typeable)
--  VectorWithNABool :: VectorWithNA Bool = VectorWithNABool (SV.Vector Bool) (SV.Vector Bool) deriving (Show, Typeable)
--  VectorWithNA :: (Typeable a) => a -> VectorWithNA a = VectorWithNA (V.Vector a) (SV.Vector Bool) deriving (Show, Typeable)
--
---- | data vector extension allowing missing values.
data family VectorWithNA a

data instance VectorWithNA Double = VectorWithNADouble (SV.Vector Double) deriving (Show)
data instance VectorWithNA Bool = VectorWithNABool (SV.Vector Bool) (SV.Vector Bool) deriving (Show)
data instance VectorWithNA a = VectorWithNA (V.Vector a) (SV.Vector Bool) deriving (Show)
data instance VectorWithNA Nominal = VectorWithNANominal deriving (Show)
data instance VectorWithNA (Constant a) = VectorWithNAConstant a deriving (Show)

data Nominal = Nominal deriving (Show)
data Constant a = Constant a deriving (Show)

-- deriving instance Typeable (VectorWithNA Double)
deriving instance Typeable VectorWithNA

--
-- data UniversalVector = forall a. Typeable a => UniversalVector (VectorWithNA a)

-- fromUniversalVector :: (Typeable a) => UniversalVector -> VectorWithNA a

-- | DataFrame (key table) (column name table) (value table)
data DataFrame = DataFrame DataTable DataTable DataTable

data DataTable = DataTable (V.Vector ByteString) (V.Vector Dynamic)
deriving instance Show DataTable

class CreateDataFrame a where
  createDataFrame :: a -> DataFrame

instance CreateDataFrame (keys, names, values) where
  createDataFrame (ks, ns, vs) = DataFrame ks ns vs


class CreateDataTableFrom a where
  createDataTableFromPair :: a -> DataTable

instance CreateDataTableFrom ([ByteString], [Dynamic]) where
  createDataTableFromPair (k, v) =
    let numNames = length k
        numValueColumns = length v
    in
        if numNames /=  numValueColumns
          then
              error "number of column names and values do not match."
          else
              DataTable (V.fromList k) (V.fromList v)

column :: (Typeable a) => DataTable -> ByteString -> Maybe (VectorWithNA a)
column dataFrame columnName =
  let DataTable names values = dataFrame
  in do
    ind <- V.findIndex (== columnName) names
    fromDynamic $ values V.! ind


class CreateDataTableFromVarPairs t where
  createDataTableFromVarPairs :: [ByteString] -> [Dynamic] -> t

instance CreateDataTableFromVarPairs DataTable where
  createDataTableFromVarPairs ks vs = createDataTableFromPair (reverse ks, reverse vs)

--deriving Show (CreateDataTableFromVarPairs DataTable)

instance (Typeable a, CreateDataTableFromVarPairs t) => CreateDataTableFromVarPairs (ByteString -> VectorWithNA a -> t) where
  createDataTableFromVarPairs ks vs k v = createDataTableFromVarPairs (k:ks) ((toDyn v):vs)
--deriving (Show a, Show t) => Show (CreateDataTableFromVarPairs (ByteString -> VectorWithNA a -> t)

createDataTable :: (CreateDataTableFromVarPairs t) => t
createDataTable = createDataTableFromVarPairs [] []

main :: IO ()
main = test1

test1 :: IO ()
test1 =
  let sdvec = SV.fromList [1 :: Double, 2, 3]
      bvec = SV.fromList [True, True, False]
      intvec = V.fromList [1 :: Int, 2, 3]
      sdvecna = VectorWithNADouble sdvec
      bvecna = VectorWithNABool bvec bvec
      intvecna = VectorWithNA intvec bvec
  in
    do
      print sdvecna
      print bvecna
      print intvecna

      print ("creating a data frame." :: ByteString)
      let df = createDataTableFromPair (["column1" :: ByteString, "column2"], [toDyn sdvecna, toDyn bvecna])
      print df
      let oneColumn = (column df ("column1" :: ByteString) :: Maybe (VectorWithNA Double))
      print oneColumn
      print ("creating data frame from var args.")
      let df2 = (createDataTable ("column1" :: ByteString) sdvecna ("column2" :: ByteString) bvecna) :: DataTable
      print df2

