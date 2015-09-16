var R = require ( 'ramda' );
var H = require ( 'highland' );
var W = require ( 'highland-wrapcallback' );
var A = require ( 'aws-sdk' );

module.exports = R.curry ( function ( s3Parms, listObjectParms ) {
    var S = new A.S3 ( s3Parms );

    var objectStream = function ( listObjectParms ) {
        return W ( S, 'listObjects' )( listObjectParms )
            .flatMap ( function ( output ) {
                return H ( output.Contents )
                    .concat ( output.IsTruncated ? objectStream ( R.merge ( listObjectParms, R.compose ( R.createMapEntry ( 'Marker' ), R.prop ( 'Key' ), R.last, R.prop ( 'Contents' ) )( output ) ) ) : H ( [] ) );
            } );
    };

    return objectStream ( listObjectParms );
};
