#pragma once

#include <core/compressed_column.hpp>
#include <cstddef>

namespace CoGaDB{


/*!
 *  \brief     This class represents a run length compressed column with type T, is the base class for all compressed typed column classes.
 */
template<class T>
class RunLengthCompressedColumn : public CompressedColumn<T>{
    public:
    /***************** constructors and destructor *****************/
    RunLengthCompressedColumn(const std::string& name, AttributeType db_type);
    virtual ~RunLengthCompressedColumn();

    virtual bool insert(const boost::any& new_Value);
    virtual bool insert(const T& new_value);
    template <typename InputIterator>
    bool insert(InputIterator first, InputIterator last);

    virtual bool update(TID tid, const boost::any& new_value);
    virtual bool update(PositionListPtr tid, const boost::any& new_value);

    virtual bool remove(TID tid);
    //assumes tid list is sorted ascending
    virtual bool remove(PositionListPtr tid);
    virtual bool clearContent();

    virtual const boost::any get(TID tid);
    //virtual const boost::any* const getRawData()=0;
    virtual void print() const throw();
    virtual size_t size() const throw();
    virtual unsigned int getSizeinBytes() const throw();

    virtual const ColumnPtr copy() const;

    virtual bool store(const std::string& path);
    virtual bool load(const std::string& path);



    virtual T& operator[](const int index);

    /*! values*/
    std::pair<std::vector<unsigned int>, std::vector<T> > runLengthColumnPair;
    std::string _name;

};


/***************** Start of Implementation Section ******************/


    template<class T>
    RunLengthCompressedColumn<T>::RunLengthCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), runLengthColumnPair(), _name(name) {

    }

    template<class T>
    RunLengthCompressedColumn<T>::~RunLengthCompressedColumn(){

    }

    template<class T>
    bool RunLengthCompressedColumn<T>::insert(const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::insert(const T& new_value) {
        if(runLengthColumnPair.first.size() == 0 || runLengthColumnPair.second.back() != new_value) {
            runLengthColumnPair.first.push_back(1);
            runLengthColumnPair.second.push_back(new_value);
        } else {
            runLengthColumnPair.first.back()++;
        }

//        std::cout << "NV: " << new_value << " FR: " << runLengthColumnPair.first.back() << " LV: " << runLengthColumnPair.second.back() << std::endl;

        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool RunLengthCompressedColumn<T>::insert(InputIterator, InputIterator){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return true;
    }

    template<class T>
    const boost::any RunLengthCompressedColumn<T>::get(TID){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return boost::any();
    }

    template<class T>
    void RunLengthCompressedColumn<T>::print() const throw(){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
    }

    template<class T>
    size_t RunLengthCompressedColumn<T>::size() const throw(){
        unsigned int counter = 0;
        for(unsigned int i = 0; i < runLengthColumnPair.first.size(); i++) {
            counter += runLengthColumnPair.first[i];
        }

        return counter;
    }

    template<class T>
    const ColumnPtr RunLengthCompressedColumn<T>::copy() const{
        return ColumnPtr(new RunLengthCompressedColumn<T>(*this));
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::update(TID tid, const boost::any& new_value){
        // Brute force approach. Recreates a simple vector representation of the column and turns back to "runLengthColumnPair" afterwards.
        // This reduces the complexity of the code for the case, that the value - that has to be updated - has a frequency value higher than 1.

        std::vector<T> simpleColumnRepresentation;
        for(unsigned int i = 0; i < runLengthColumnPair.first.size(); i++) {
            for(unsigned int j = 0; j < runLengthColumnPair.first[i]; j++) {
                simpleColumnRepresentation.push_back(runLengthColumnPair.second[i]);
            }
        }

        if(tid >= simpleColumnRepresentation.size()) {
            return false;
        }

        simpleColumnRepresentation[tid] = boost::any_cast<T>(new_value);

        // Restore the Run Length Compressed Column representation (runLengthColumnPair)
        clearContent();
        for(unsigned int i = 0; i < simpleColumnRepresentation.size(); i++) {
            insert(simpleColumnRepresentation[i]);
        }

        return true;

        // In this former code, we were not sure how to implement the behavior for the case, that: runLengthColumnPair.first[i] != 1
//        unsigned int counter = 0;
//        for(unsigned int i = 0; i < runLengthColumnPair.first.size(); i++) {
//            for(unsigned int j = 0; j < runLengthColumnPair.first[i]; j++) {
//                if(counter == tid) {
//                    if(runLengthColumnPair.first[i] == 1) {
//                        runLengthColumnPair.second[i] = boost::any_cast<T>(new_value);
//                    } else {
//                        // Add behavior.
//                    }
//                    return true;
//                }
//                counter++;
//            }
//        }
//        return false;
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::update(PositionListPtr, const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::remove(TID tid){
        unsigned int counter = 0;
        for(unsigned int i = 0; i < runLengthColumnPair.first.size(); i++) {
            for(unsigned int j = 0; j < runLengthColumnPair.first[i]; j++) {
                if(counter == tid) {
                    if(runLengthColumnPair.first[i] == 1) {
                        runLengthColumnPair.first.erase(runLengthColumnPair.first.begin() + i);
                        runLengthColumnPair.second.erase(runLengthColumnPair.second.begin() + i);
                    } else {
                        runLengthColumnPair.first[i]--;
                    }
                    return true;
                }
                counter++;
            }
        }

        return false;
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::remove(PositionListPtr){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::clearContent(){
        runLengthColumnPair.first.clear();
        runLengthColumnPair.second.clear();
        return true;
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::store(const std::string& path_){
        // Recreate normal/standard column form?

        std::string path(path_);
        path += _name;

//        std::cout << "Writing Column '" << _name << "' to File '" << path << "'" << std::endl;

        std::ofstream outfile (path.c_str(), std::fstream::out | std::fstream::binary);
        boost::archive::binary_oarchive oa(outfile);

//        oa << runLengthColumnPair;

        // REMAPS TO THE INITIAL VALUES. PLACEHOLDER UNTIL REAL REQUIREMENTS FOR THIS METHOD ARE CLEAR TO US.
        std::vector<T> simpleColumnRepresentation;
        for(unsigned int i = 0; i < runLengthColumnPair.first.size(); i++) {
            for(unsigned int j = 0; j < runLengthColumnPair.first[i]; j++) {
                simpleColumnRepresentation.push_back(runLengthColumnPair.second[i]);
            }
        }
        oa << simpleColumnRepresentation;

        outfile.flush();
        outfile.close();

//        for(unsigned int i = 0; i < simpleColumnRepresentation.size(); i++) {
//            std::cout << "Column entry[" << i << "]: " << simpleColumnRepresentation[i] << std::endl;
//        }

        return true;
    }

    template<class T>
    bool RunLengthCompressedColumn<T>::load(const std::string& path_){
        std::string path(path_);
        path += _name;

        std::ifstream infile (path.c_str(), std::fstream::in | std::fstream::binary);
        boost::archive::binary_iarchive ia(infile);

//        ia >> runLengthColumnPair;
        infile.close();

        return true;
    }

    template<class T>
    T& RunLengthCompressedColumn<T>::operator[](const int index){
        int counter = 0;
        for(unsigned int i = 0; i < runLengthColumnPair.first.size(); i++) {
            counter += runLengthColumnPair.first[i];
            if(counter > index) {
                return runLengthColumnPair.second[i];
            }
        }

        // Will result in an exception (out of bounds). This will point to a problem in this method (if there should be one).
        return runLengthColumnPair.second[runLengthColumnPair.second.size()];
    }

    template<class T>
    unsigned int RunLengthCompressedColumn<T>::getSizeinBytes() const throw(){
        // What shall be returned? Size of runLengthColumnPair?
        return 10;
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

